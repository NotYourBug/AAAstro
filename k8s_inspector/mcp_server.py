"""
基于 FastMCP 的精简版 K8s 巡检 MCP Server。

设计目标：
- 不是把整个项目都改造成 MCP，而是只保留“check/排障”核心逻辑
- 重点暴露给 LLM 的是：节点状态、节点详情、节点资源审计、异常节点识别
- 额外提供：节点上的 Pod 查询、Pod 异常诊断、集群摘要 Resource、排障 Prompt

这份 MCP Server 当前是独立自包含的实现：
- parse_quantity：在本文件内统一处理 K8s 资源单位（m / Mi / Gi ...）
- _pod_issue / _BAD_POD_REASONS：在本文件内完成 Pod 异常判定
- InspectorConfig：在本文件内维护阈值配置，便于单独移植

运行方式：
- 推荐单独使用 Python 3.10+ 虚拟环境安装 fastmcp
- 启动后由 Claude / Trae / 其他 MCP Client 以 stdio 或 HTTP 方式接入
"""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Any

from fastmcp import FastMCP
from kubernetes import client, config
from kubernetes.client import ApiClient
from kubernetes.client.exceptions import ApiException
from pydantic import Field

# MCP Server 主对象：用于注册 tools/resources/prompts，并对外运行协议服务。
mcp = FastMCP("K8s Check MCP")


@dataclass
class MCPPodResource:
    """单个 Pod 的有效资源画像（调度视角）。"""

    cpu_request_m: int = 0
    cpu_limit_m: int = 0
    memory_request_bytes: int = 0
    memory_limit_bytes: int = 0


@dataclass
class MCPKubeClients:
    """MCP Server 用到的 K8s API 客户端集合。"""

    display_name: str
    api_client: ApiClient
    core: client.CoreV1Api
    custom: client.CustomObjectsApi


@dataclass
class Thresholds:
    """诊断阈值集合（默认值更偏教学/示例，可按环境变量覆盖）。"""

    node_cpu_utilization: float = 0.80
    node_memory_utilization: float = 0.85
    pod_restart_count: int = 3
    requests_overcommit_ratio: float = 1.0
    limits_overcommit_ratio: float = 1.5


@dataclass
class InspectorConfig:
    """
    MCP Server 的运行配置（精简版）。

    与原巡检项目解耦：
    - 这里不依赖任何其他模块
    - 仅保留 MCP 工具需要的阈值与少量开关
    """

    thresholds: Thresholds = field(default_factory=Thresholds)


def _env_bool(name: str, default: bool = False) -> bool:
    """从环境变量读取布尔值，允许 1/true/yes/on 等常见写法。"""
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw.strip())
    except Exception:
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw.strip())
    except Exception:
        return default

'''
K8s API 调用速率限制器（令牌桶算法）-- 三层限流保护
1. 令牌桶限流（控制RPS = 每秒请求数）
2. 最大突发burst（控制瞬间流量）
3. 最大并发数max_in_flight（控制同时进行中的调用数）
是同步线程安全版本（不是异步），用于限制kubernetes API请求频率
'''
class _TokenBucketLimiter:
    def __init__(self, rps: float, burst: int, max_in_flight: int) -> None:
        self._rps = float(rps)                  # 每秒生成多少令牌（核心数率） 
        self._capacity = max(int(burst), 1)     # 桶的最大容量（突发上限）
        self._tokens = float(self._capacity)    # 当前令牌数（初始满桶）
        self._updated_at = time.monotonic()     # 上一次更新令牌时间（用于计算令牌生成）
        self._lock = threading.Lock()           # 线程锁，保证多线程安全修改令牌
        self._sem = threading.Semaphore(max(int(max_in_flight), 1))  # 最大并发数信号量

    # 拿令牌 + 强并发名额
    def acquire(self) -> None:
        self._sem.acquire()     # 【第一步：先抢并发名额 → 最多3个同时运行】
        try:
            if self._rps <= 0:
                return
            while True:
                wait_s = 0.0
                with self._lock:  # 加锁，多线程安全修改令牌
                    now = time.monotonic()
                    elapsed = max(now - self._updated_at, 0.0)  # 距离上次更新过了多少时间
                    # 【核心：根据时间差计算并填充令牌】
                    if elapsed > 0:
                        self._tokens = min(self._capacity, self._tokens + elapsed * self._rps) # 时间 x 速率
                        self._updated_at = now
                    # 【有令牌 → 拿走一个，直接返回】
                    if self._tokens >= 1.0:
                        self._tokens -= 1.0
                        return
                    # 【没令牌 → 计算还需要等多久才能生成1个令牌】
                    wait_s = (1.0 - self._tokens) / self._rps
                time.sleep(max(wait_s, 0.001))  # 【等待一段时间再重试：至少0.001秒，避免CPU占用过高】
        except Exception:
            self._sem.release()
            raise

    def release(self) -> None:
        self._sem.release()  # 【第二步：释放并发名额，下一个请求可以进来】


_API_LIMITER: _TokenBucketLimiter | None = None

# 全局单例，唯一限流器，所有K8s API共用一套限流规则
def _get_api_limiter() -> _TokenBucketLimiter:
    global _API_LIMITER
    if _API_LIMITER is not None:
        return _API_LIMITER
    # 从环境变量中读取限流配置
    rps = _env_float("MCP_K8S_API_RPS", 5.0)
    burst = _env_int("MCP_K8S_API_BURST", 10)
    max_in_flight = _env_int("MCP_K8S_API_MAX_IN_FLIGHT", 3)
    _API_LIMITER = _TokenBucketLimiter(rps=rps, burst=burst, max_in_flight=max_in_flight)
    return _API_LIMITER

# 包装函数，所有K8s API调用都会经过这里
def _k8s_api_call(fn: Any, *args: Any, **kwargs: Any) -> Any:
    limiter = _get_api_limiter()
    limiter.acquire()
    try:
        return fn(*args, **kwargs)
    finally:
        limiter.release()


def _load_config_file(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    p = Path(path)
    raw = p.read_text(encoding="utf-8")
    if p.suffix.lower() in {".yaml", ".yml"}:
        try:
            import yaml  # type: ignore
        except Exception as e:
            raise RuntimeError("读取 YAML 配置需要安装 PyYAML") from e
        data = yaml.safe_load(raw) or {}
        return data if isinstance(data, dict) else {}
    data = json.loads(raw) if raw.strip() else {}
    return data if isinstance(data, dict) else {}


def _deep_get(d: dict[str, Any], path: list[str], default: Any) -> Any:
    cur: Any = d
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def _get_runtime_config() -> InspectorConfig:
    """
    读取 MCP Server 运行配置。

    优先级：
    1) MCP_CONFIG（JSON/YAML）
    2) 环境变量（MCP_THRESHOLD_*）
    3) 内置默认值
    """

    cfg = InspectorConfig()
    data = _load_config_file((os.getenv("MCP_CONFIG") or "").strip() or None)

    t = cfg.thresholds
    t.node_cpu_utilization = float(
        _deep_get(data, ["thresholds", "node_cpu_utilization"], t.node_cpu_utilization)
    )
    t.node_memory_utilization = float(
        _deep_get(data, ["thresholds", "node_memory_utilization"], t.node_memory_utilization)
    )
    t.pod_restart_count = int(_deep_get(data, ["thresholds", "pod_restart_count"], t.pod_restart_count))
    t.requests_overcommit_ratio = float(
        _deep_get(data, ["thresholds", "requests_overcommit_ratio"], t.requests_overcommit_ratio)
    )
    t.limits_overcommit_ratio = float(
        _deep_get(data, ["thresholds", "limits_overcommit_ratio"], t.limits_overcommit_ratio)
    )

    t.node_cpu_utilization = _env_float("MCP_THRESHOLD_NODE_CPU_UTILIZATION", t.node_cpu_utilization)
    t.node_memory_utilization = _env_float(
        "MCP_THRESHOLD_NODE_MEMORY_UTILIZATION", t.node_memory_utilization
    )
    t.pod_restart_count = _env_int("MCP_THRESHOLD_POD_RESTART_COUNT", t.pod_restart_count)
    t.requests_overcommit_ratio = _env_float(
        "MCP_THRESHOLD_REQUESTS_OVERCOMMIT_RATIO", t.requests_overcommit_ratio
    )
    t.limits_overcommit_ratio = _env_float("MCP_THRESHOLD_LIMITS_OVERCOMMIT_RATIO", t.limits_overcommit_ratio)

    return cfg


def init_k8s_client() -> MCPKubeClients:
    """
    初始化 MCP Server 使用的 Kubernetes 客户端。

    环境变量：
    - MCP_K8S_IN_CLUSTER=true/false
    - MCP_K8S_KUBECONFIG=/path/to/config
    - MCP_K8S_CONTEXT=xxx
    - MCP_K8S_VERIFY_SSL=true/false
    """

    # 双模式兼容：既支持本地 kubeconfig，也支持集群内 ServiceAccount。
    in_cluster = _env_bool("MCP_K8S_IN_CLUSTER", False)
    kubeconfig = (os.getenv("MCP_K8S_KUBECONFIG") or "").strip() or None
    context = (os.getenv("MCP_K8S_CONTEXT") or "").strip() or None

    # 证书校验开关：教学/测试环境经常关闭；生产环境建议开启。
    verify_ssl = _env_bool("MCP_K8S_VERIFY_SSL", False)

    if in_cluster:
        config.load_incluster_config()
        display_name = "in-cluster"
    else:
        config.load_kube_config(config_file=kubeconfig, context=context)
        display_name = context or "kubeconfig"

    cfg = client.Configuration.get_default_copy()
    cfg.verify_ssl = verify_ssl
    api_client = client.ApiClient(cfg)
    return MCPKubeClients(
        display_name=display_name,
        api_client=api_client,
        core=client.CoreV1Api(api_client),
        custom=client.CustomObjectsApi(api_client),
    )


def _now_utc() -> datetime:
    """返回 UTC 当前时间（用于计算资源年龄等）。"""
    return datetime.now(timezone.utc)


def _format_age(creation_timestamp: datetime | None) -> str:
    """把创建时间转换成紧凑的 age（m/h/d），用于节点列表展示。"""
    if creation_timestamp is None:
        return ""
    delta = _now_utc() - creation_timestamp.astimezone(timezone.utc)
    total_seconds = int(delta.total_seconds())
    if total_seconds < 3600:
        return f"{max(total_seconds // 60, 0)}m"
    if total_seconds < 86400:
        return f"{total_seconds // 3600}h"
    return f"{total_seconds // 86400}d"


def _safe_name(obj: Any) -> str:
    """安全获取资源对象名称（metadata.name），避免空对象导致异常。"""
    metadata = getattr(obj, "metadata", None)
    return getattr(metadata, "name", "") or ""
# getattr是用来动态获取、设置对象的属性，常用于反射、动态编程。
# getattr(object, name, default=None)

_DECIMAL = {
    "n": 1e-9,
    "u": 1e-6,
    "m": 1e-3,
    "": 1.0,
    "k": 1e3,
    "M": 1e6,
    "G": 1e9,
    "T": 1e12,
    "P": 1e15,
    "E": 1e18,
}

_BINARY = {
    "Ki": 1024.0,
    "Mi": 1024.0**2,
    "Gi": 1024.0**3,
    "Ti": 1024.0**4,
    "Pi": 1024.0**5,
    "Ei": 1024.0**6,
}


def parse_quantity(q: str) -> float:
    """
    解析 K8s resource quantity 字符串为数值。

    返回单位：
    - cpu：以“核”为基准（m 表示 1e-3）
    - memory：以“字节”为基准（Ki/Mi/Gi 等为 1024 进位）
    """

    s = (q or "").strip()
    if not s:
        return 0.0
    for suf, mult in _BINARY.items():
        if s.endswith(suf):
            return float(s[: -len(suf)]) * mult
    for suf, mult in _DECIMAL.items():
        if suf and s.endswith(suf):
            return float(s[: -len(suf)]) * mult
    return float(s)



def _cpu_to_millicores(quantity: str) -> int:
    """把 K8s CPU quantity（如 250m/1）统一换算成 millicores（m）。"""
    return int(round(parse_quantity(quantity) * 1000))


def _memory_to_bytes(quantity: str) -> int:
    """把 K8s 内存 quantity（如 128Mi/1Gi）统一换算成 bytes。"""
    return int(round(parse_quantity(quantity)))


def _format_cpu_m(cpu_m: int) -> str:
    """把 millicores 转成可读字符串。"""
    return f"{cpu_m}m"


def _format_bytes(num_bytes: int) -> str:
    """把 bytes 转成 Ki/Mi/Gi 形式的可读字符串（用于输出展示）。"""
    if num_bytes < 1024:
        return f"{num_bytes}B"
    units = ["Ki", "Mi", "Gi", "Ti", "Pi"]
    value = float(num_bytes)
    for unit in units:
        value /= 1024.0
        if value < 1024.0:
            return f"{value:.1f}{unit}"
    return f"{value:.1f}Ei"


def _ratio(numerator: int, denominator: int) -> float:
    """计算占比，避免除 0，并做 4 位小数截断。"""
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 4)


def _get_node_addresses(node: client.V1Node) -> dict[str, str]:
    """提取 Node 地址（InternalIP/Hostname 等）为字典，便于展示与过滤。"""
    addresses: dict[str, str] = {}
    for addr in node.status.addresses or []:
        addr_type = addr.type or "Unknown"
        addr_value = addr.address or ""
        if addr_value:
            addresses[addr_type] = addr_value
    return addresses


def _get_conditions(node: client.V1Node) -> dict[str, str]:
    """提取 Node Conditions（Ready/DiskPressure/...）为扁平化字典。"""
    conditions: dict[str, str] = {}
    for cond in node.status.conditions or []:
        conditions[cond.type or "Unknown"] = cond.status or "Unknown"
    return conditions


def _taints_to_list(node: client.V1Node) -> list[str]:
    """把 taints 转成更直观的一维列表字符串，便于 LLM 阅读。"""
    out: list[str] = []
    for taint in node.spec.taints or []:
        key = taint.key or ""
        effect = taint.effect or ""
        value = taint.value or ""
        if value:
            out.append(f"{key}={value}:{effect}")
            
        else:
            out.append(f"{key}:{effect}")
    return out


def _parse_node(node: client.V1Node) -> dict[str, Any]:
    """把复杂 Node 对象压平为更适合 LLM 阅读的结构。"""
    conditions = _get_conditions(node)
    addresses = _get_node_addresses(node)
    info = node.status.node_info if node.status else None
    allocatable = node.status.allocatable if node.status else {}
    capacity = node.status.capacity if node.status else {}
    return {
        "name": _safe_name(node),
        "status": "Ready" if conditions.get("Ready") == "True" else "NotReady",
        "age": _format_age(getattr(node.metadata, "creation_timestamp", None)),
        "internal_ip": addresses.get("InternalIP", ""),
        "hostname": addresses.get("Hostname", ""),
        "unschedulable": bool(getattr(node.spec, "unschedulable", False)),
        "taints": _taints_to_list(node),
        "conditions": conditions,
        "os_image": getattr(info, "os_image", "") or "",
        "kernel_version": getattr(info, "kernel_version", "") or "",
        "kubelet_version": getattr(info, "kubelet_version", "") or "",
        "container_runtime": getattr(info, "container_runtime_version", "") or "",
        "capacity": {
            "cpu": str(capacity.get("cpu", "")),
            "memory": str(capacity.get("memory", "")),
            "pods": str(capacity.get("pods", "")),
        },
        "allocatable": {
            "cpu": str(allocatable.get("cpu", "")),
            "memory": str(allocatable.get("memory", "")),
            "pods": str(allocatable.get("pods", "")),
        },
    }


def _active_pods_on_node(core: client.CoreV1Api, node_name: str) -> list[client.V1Pod]:
    """
    只统计已调度到节点上、且非终态的 Pod。

    这样更接近“当前占用该节点调度资源”的视角。
    """

    pods = _k8s_api_call(
        core.list_pod_for_all_namespaces,
        field_selector=f"spec.nodeName={node_name}",
        watch=False,
    ).items
    out: list[client.V1Pod] = []
    for pod in pods:
        phase = pod.status.phase if pod.status else ""
        # 终态 Pod（Succeeded/Failed）不再占用调度资源，资源审计时可以排除。
        if phase in {"Succeeded", "Failed"}:
            continue
        out.append(pod)
    return out


def _sum_container_resources(containers: list[Any]) -> tuple[int, int, int, int]:
    """对一组容器的 requests/limits 进行求和（CPU m / Memory bytes）。"""
    cpu_request_m = 0
    cpu_limit_m = 0
    mem_request_b = 0
    mem_limit_b = 0
    for container in containers:
        resources = getattr(container, "resources", None)
        requests = getattr(resources, "requests", None) or {}
        limits = getattr(resources, "limits", None) or {}
        cpu_request_m += _cpu_to_millicores(str(requests.get("cpu", "0")))
        cpu_limit_m += _cpu_to_millicores(str(limits.get("cpu", "0")))
        mem_request_b += _memory_to_bytes(str(requests.get("memory", "0")))
        mem_limit_b += _memory_to_bytes(str(limits.get("memory", "0")))
    return cpu_request_m, cpu_limit_m, mem_request_b, mem_limit_b


def _pod_effective_resources(pod: client.V1Pod) -> MCPPodResource:
    """
    计算单个 Pod 的有效 Requests/Limits。

    调度视角下，InitContainer 采用 max(init) 与 sum(containers) 的组合逻辑。
    """

    containers = pod.spec.containers or []
    init_containers = pod.spec.init_containers or []

    c_req_cpu, c_lim_cpu, c_req_mem, c_lim_mem = _sum_container_resources(containers)
    i_req_cpu, i_lim_cpu, i_req_mem, i_lim_mem = _sum_container_resources(init_containers)

    # K8s 调度口径：
    # - 普通容器资源是 sum(containers)
    # - initContainers 是按“串行执行”计算峰值，取 max(initContainers)
    # 这里用 max(sum(containers), max(init)) 实现该口径的简化版本。
    return MCPPodResource(
        cpu_request_m=max(c_req_cpu, i_req_cpu),
        cpu_limit_m=max(c_lim_cpu, i_lim_cpu),
        memory_request_bytes=max(c_req_mem, i_req_mem),
        memory_limit_bytes=max(c_lim_mem, i_lim_mem),
    )


def _pod_restart_count(pod: client.V1Pod) -> int:
    """累计 Pod 中所有容器的 restartCount，作为“重启偏高”的基础指标。"""
    total = 0
    for cs in pod.status.container_statuses or []:
        total += int(cs.restart_count or 0)
    return total


def _pod_qos_class(pod: client.V1Pod) -> str:
    """读取 Pod QoS Class（Guaranteed/Burstable/BestEffort），用于资源/驱逐建议。"""
    if pod.status and pod.status.qos_class:
        return pod.status.qos_class
    return "Unknown"


def _pod_basic_view(pod: client.V1Pod) -> dict[str, Any]:
    """Pod 的最小摘要结构（用于列表/样例），避免把完整对象直接暴露给 LLM。"""
    return {
        "namespace": getattr(pod.metadata, "namespace", "") or "",
        "name": getattr(pod.metadata, "name", "") or "",
        "phase": getattr(pod.status, "phase", "") or "",
        "qos_class": _pod_qos_class(pod),
        "restart_count": _pod_restart_count(pod),
        "issue": _pod_issue(pod, restart_threshold=0),
    }


def _node_metric_view(custom: client.CustomObjectsApi, node_name: str) -> dict[str, Any] | None:
    """读取 metrics-server 的 Node usage（如果不可用则返回 None）。"""
    metrics = get_node_metrics(custom)
    if not metrics:
        return None
    usage = metrics.get(node_name)
    if not usage:
        return None
    cpu_m = _cpu_to_millicores(str(usage.get("cpu", "0")))
    mem_b = _memory_to_bytes(str(usage.get("memory", "0")))
    return {
        "cpu_usage_m": cpu_m,
        "cpu_usage_human": _format_cpu_m(cpu_m),
        "memory_usage_bytes": mem_b,
        "memory_usage_human": _format_bytes(mem_b),
    }

def get_node_metrics(custom: client.CustomObjectsApi) -> dict[str, dict[str, str]] | None:
    """
    读取 metrics.k8s.io 的 Node 指标。

    - 若集群未安装 metrics-server 或无权限，返回 None（上层应降级处理）
    - 返回格式：
      {
        "node-1": {"cpu": "123m", "memory": "456Mi"},
        ...
      }
    """

    try:
        obj = _k8s_api_call(
            custom.list_cluster_custom_object,
            group="metrics.k8s.io",
            version="v1beta1",
            plural="nodes",
        )
    except Exception:
        return None

    items = obj.get("items") if isinstance(obj, dict) else None
    if not isinstance(items, list):
        return None

    out: dict[str, dict[str, str]] = {}
    for it in items:
        md = it.get("metadata", {}) if isinstance(it, dict) else {}
        name = md.get("name")
        usage = it.get("usage", {})
        if isinstance(name, str) and isinstance(usage, dict):
            out[name] = {
                "cpu": str(usage.get("cpu", "")),
                "memory": str(usage.get("memory", "")),
            }
    return out


_BAD_POD_REASONS = {
    "CrashLoopBackOff",
    "ImagePullBackOff",
    "ErrImagePull",
    "CreateContainerConfigError",
    "CreateContainerError",
    "RunContainerError",
    "InvalidImageName",
    "OOMKilled",
}


def _pod_issue(pod: client.V1Pod, restart_threshold: int) -> str | None:
    """
    判断 Pod 是否存在“明显异常信号”。

    返回：
    - None：未发现问题
    - str：问题描述（用于上层收集进结果）
    """

    if not pod.status:
        return "missing_status"
    phase = pod.status.phase or ""
    if phase in {"Failed", "Unknown"}:
        return f"phase={phase}"

    cs_list = pod.status.container_statuses or []
    for cs in cs_list:
        if cs.state and cs.state.waiting and cs.state.waiting.reason in _BAD_POD_REASONS:
            return f"waiting={cs.state.waiting.reason}"
        if cs.last_state and cs.last_state.terminated and cs.last_state.terminated.reason in _BAD_POD_REASONS:
            return f"terminated={cs.last_state.terminated.reason}"

    if restart_threshold > 0:
        for cs in cs_list:
            if (cs.restart_count or 0) >= restart_threshold:
                return f"restart_count>={restart_threshold}"
    return None



def _problem_reason_list(
    node: client.V1Node,
    custom: client.CustomObjectsApi,
    cfg: InspectorConfig,
) -> tuple[list[str], dict[str, Any]]:
    """汇总“该节点为什么可疑”的原因列表，并附带可选的 metrics 详情。"""
    conditions = _get_conditions(node)
    reasons: list[str] = []
    metric_detail: dict[str, Any] = {}
    node_name = _safe_name(node)

    if conditions.get("Ready") != "True":
        reasons.append("NodeNotReady")
    if conditions.get("DiskPressure") == "True":
        reasons.append("DiskPressure")
    if conditions.get("MemoryPressure") == "True":
        reasons.append("MemoryPressure")
    if conditions.get("PIDPressure") == "True":
        reasons.append("PIDPressure")
    if conditions.get("NetworkUnavailable") == "True":
        reasons.append("NetworkUnavailable")
    if bool(getattr(node.spec, "unschedulable", False)):
        reasons.append("Unschedulable")

    # metrics-server 可用时，再根据 usage/capacity 判断是否高负载（HighCPU/HighMemory）。
    metrics = _node_metric_view(custom, node_name)
    capacity = node.status.capacity if node.status else {}
    if metrics and capacity:
        cpu_cap_m = _cpu_to_millicores(str(capacity.get("cpu", "0")))
        mem_cap_b = _memory_to_bytes(str(capacity.get("memory", "0")))
        cpu_ratio = _ratio(metrics["cpu_usage_m"], cpu_cap_m)
        mem_ratio = _ratio(metrics["memory_usage_bytes"], mem_cap_b)
        metric_detail = {
            **metrics,
            "cpu_capacity_m": cpu_cap_m,
            "memory_capacity_bytes": mem_cap_b,
            "cpu_utilization": cpu_ratio,
            "memory_utilization": mem_ratio,
        }
        if cpu_ratio >= cfg.thresholds.node_cpu_utilization:
            reasons.append("HighCPUUsage")
        if mem_ratio >= cfg.thresholds.node_memory_utilization:
            reasons.append("HighMemoryUsage")

    return reasons, metric_detail


def _event_brief(event: client.V1Event) -> dict[str, str]:
    """把 Event 压缩成短结构（原因/类型/消息/时间戳），用于 Pod 诊断输出。"""
    return {
        "reason": event.reason or "",
        "type": event.type or "",
        "message": (event.message or "")[:300],
        "last_timestamp": (
            str(getattr(event, "last_timestamp", "") or getattr(event, "event_time", "") or "")
        ),
    }


def _pod_issue_summary(pod: client.V1Pod, restart_threshold: int) -> str | None:
    """生成更适合列表展示的 issue 字段：先看硬故障，再看重启偏高。"""
    issue = _pod_issue(pod, restart_threshold=0)
    if issue:
        return issue
    heavy = _pod_issue(pod, restart_threshold=restart_threshold)
    if heavy == f"restart_count>={restart_threshold}":
        return heavy
    return None


def _pod_suggestions(issue: str | None, phase: str) -> list[str]:
    """把常见 issue/phase 映射成可操作的排障建议（给 LLM 作为输出参考）。"""
    if issue is None and phase == "Running":
        return ["Pod 当前未发现明显异常，可继续检查依赖服务、探针与业务日志。"]
    if issue is None and phase == "Pending":
        return ["重点查看调度失败事件、PVC 绑定状态、镜像拉取与节点资源余量。"]

    out: list[str] = []
    if issue and "CrashLoopBackOff" in issue:
        out.append("优先查看 previous 日志，确认应用启动异常、配置错误或依赖不可达。")
        out.append("同时检查探针、环境变量、Secret/ConfigMap 挂载是否正确。")
    if issue and ("ImagePullBackOff" in issue or "ErrImagePull" in issue):
        out.append("检查镜像地址、tag、镜像仓库凭据以及节点到仓库的网络连通性。")
    if issue and "OOMKilled" in issue:
        out.append("检查容器内存 limit/request 是否过低，以及应用是否存在内存泄漏。")
    if issue and "restart_count" in issue:
        out.append("容器重启偏多，建议结合事件与日志确认是否为瞬时抖动还是持续性故障。")
    if phase == "Pending":
        out.append("Pending 优先看调度事件，例如资源不足、污点不容忍、PVC 未绑定。")
    if not out:
        out.append("结合 Pod 事件、容器状态与最近日志进一步确认根因。")
    return out


@mcp.tool(
    annotations={
        "title": "List Kubernetes Nodes",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": True,
    }
)
def list_nodes(
    label_selector: Annotated[
        str | None,
        Field(description="可选的 Kubernetes label selector，例如 node-role.kubernetes.io/worker"),
    ] = None,
) -> dict[str, Any]:
    """列出集群中的节点，并返回适合 LLM 阅读的扁平化摘要。"""

    kube = init_k8s_client()
    nodes = _k8s_api_call(kube.core.list_node, label_selector=label_selector or None).items
    items = [_parse_node(node) for node in nodes]
    return {
        "cluster": kube.display_name,
        "count": len(items),
        "items": items,
    }


@mcp.tool(
    annotations={
        "title": "Get Node Detail",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": True,
    }
)
def get_node_detail(
    node_name: Annotated[str, Field(description="节点名称")],
) -> dict[str, Any]:
    """获取单个节点的详细信息，包括基础属性、实时指标与节点上 Pod 概览。"""

    kube = init_k8s_client()
    node = _k8s_api_call(kube.core.read_node, node_name)
    pods = _active_pods_on_node(kube.core, node_name)
    metrics = _node_metric_view(kube.custom, node_name)
    detail = _parse_node(node)
    detail["pod_count"] = len(pods)
    detail["pods_sample"] = [_pod_basic_view(pod) for pod in pods[:20]]
    detail["metrics"] = metrics
    return detail


@mcp.tool(
    annotations={
        "title": "Audit Node Resource Usage",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": True,
    }
)
def get_node_resource_usage(
    node_name: Annotated[str, Field(description="节点名称")],
) -> dict[str, Any]:
    """
    审计节点上的 Pod Requests/Limits 水位，并在请求超过 100% 时给出 BestEffort Pod 迁移建议。
    """

    kube = init_k8s_client()
    cfg = _get_runtime_config()
    node = _k8s_api_call(kube.core.read_node, node_name)
    pods = _active_pods_on_node(kube.core, node_name)
    allocatable = node.status.allocatable if node.status else {}
    capacity = node.status.capacity if node.status else {}

    total_cpu_req_m = 0
    total_cpu_lim_m = 0
    total_mem_req_b = 0
    total_mem_lim_b = 0
    pod_breakdown: list[dict[str, Any]] = []
    best_effort_pods: list[dict[str, str]] = []

    for pod in pods:
        # 逐 Pod 汇总 requests/limits，并保留有限数量的 breakdown 供 LLM 下钻。
        pod_res = _pod_effective_resources(pod)
        total_cpu_req_m += pod_res.cpu_request_m
        total_cpu_lim_m += pod_res.cpu_limit_m
        total_mem_req_b += pod_res.memory_request_bytes
        total_mem_lim_b += pod_res.memory_limit_bytes

        qos_class = _pod_qos_class(pod)
        if qos_class == "BestEffort":
            best_effort_pods.append(
                {
                    "namespace": getattr(pod.metadata, "namespace", "") or "",
                    "name": getattr(pod.metadata, "name", "") or "",
                }
            )

        pod_breakdown.append(
            {
                "namespace": getattr(pod.metadata, "namespace", "") or "",
                "name": getattr(pod.metadata, "name", "") or "",
                "qos_class": qos_class,
                "cpu_request_m": pod_res.cpu_request_m,
                "cpu_limit_m": pod_res.cpu_limit_m,
                "memory_request_bytes": pod_res.memory_request_bytes,
                "memory_limit_bytes": pod_res.memory_limit_bytes,
                "restart_count": _pod_restart_count(pod),
            }
        )

    alloc_cpu_m = _cpu_to_millicores(str(allocatable.get("cpu", "0")))
    alloc_mem_b = _memory_to_bytes(str(allocatable.get("memory", "0")))
    cap_cpu_m = _cpu_to_millicores(str(capacity.get("cpu", "0")))
    cap_mem_b = _memory_to_bytes(str(capacity.get("memory", "0")))

    cpu_req_ratio = _ratio(total_cpu_req_m, alloc_cpu_m)
    mem_req_ratio = _ratio(total_mem_req_b, alloc_mem_b)
    cpu_lim_ratio = _ratio(total_cpu_lim_m, alloc_cpu_m)
    mem_lim_ratio = _ratio(total_mem_lim_b, alloc_mem_b)

    metric_view = _node_metric_view(kube.custom, node_name)
    eviction_suggestion = None
    requests_overcommit = (
        cpu_req_ratio > cfg.thresholds.requests_overcommit_ratio
        or mem_req_ratio > cfg.thresholds.requests_overcommit_ratio
    )
    limits_overcommit = (
        cpu_lim_ratio > cfg.thresholds.limits_overcommit_ratio
        or mem_lim_ratio > cfg.thresholds.limits_overcommit_ratio
    )
    limits_warning = None
    if limits_overcommit:
        limits_warning = {
            "reason": "节点 Limits 水位偏高（软风险），建议结合实际负载与 QoS 分布评估是否需要扩容或做工作负载重平衡。",
            "threshold": cfg.thresholds.limits_overcommit_ratio,
        }

    if requests_overcommit:
        eviction_suggestion = {
            "reason": "节点已出现 requests overcommit，建议优先评估 BestEffort Pod 的迁移或驱逐影响。",
            "best_effort_candidates": best_effort_pods,
            "threshold": cfg.thresholds.requests_overcommit_ratio,
        }

    return {
        "node": node_name,
        "cluster": kube.display_name,
        "pod_count": len(pods),
        "allocatable": {
            "cpu_m": alloc_cpu_m,
            "cpu_human": _format_cpu_m(alloc_cpu_m),
            "memory_bytes": alloc_mem_b,
            "memory_human": _format_bytes(alloc_mem_b),
        },
        "capacity": {
            "cpu_m": cap_cpu_m,
            "cpu_human": _format_cpu_m(cap_cpu_m),
            "memory_bytes": cap_mem_b,
            "memory_human": _format_bytes(cap_mem_b),
        },
        "requests": {
            "cpu_m": total_cpu_req_m,
            "cpu_human": _format_cpu_m(total_cpu_req_m),
            "memory_bytes": total_mem_req_b,
            "memory_human": _format_bytes(total_mem_req_b),
            "cpu_ratio": cpu_req_ratio,
            "memory_ratio": mem_req_ratio,
        },
        "limits": {
            "cpu_m": total_cpu_lim_m,
            "cpu_human": _format_cpu_m(total_cpu_lim_m),
            "memory_bytes": total_mem_lim_b,
            "memory_human": _format_bytes(total_mem_lim_b),
            "cpu_ratio": cpu_lim_ratio,
            "memory_ratio": mem_lim_ratio,
        },
        "live_usage": metric_view,
        "overcommit": {
            "requests": requests_overcommit,
            "limits": limits_overcommit,
            "requests_threshold": cfg.thresholds.requests_overcommit_ratio,
            "limits_threshold": cfg.thresholds.limits_overcommit_ratio,
        },
        "eviction_suggestion": eviction_suggestion,
        "limits_warning": limits_warning,
        "pod_breakdown": pod_breakdown[:50],
    }


@mcp.tool(
    annotations={
        "title": "Find Problem Nodes",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": True,
    }
)
def find_problem_nodes(
    label_selector: Annotated[
        str | None,
        Field(description="可选的节点标签过滤条件"),
    ] = None,
) -> dict[str, Any]:
    """扫描异常节点，自动识别 NotReady、磁盘/内存压力以及高负载节点。"""

    kube = init_k8s_client()
    cfg = _get_runtime_config()
    nodes = _k8s_api_call(kube.core.list_node, label_selector=label_selector or None).items
    items: list[dict[str, Any]] = []

    for node in nodes:
        reasons, metric_detail = _problem_reason_list(node, kube.custom, cfg)
        if not reasons:
            continue
        items.append(
            {
                "name": _safe_name(node),
                "status": "Ready" if _get_conditions(node).get("Ready") == "True" else "NotReady",
                "internal_ip": _get_node_addresses(node).get("InternalIP", ""),
                "age": _format_age(getattr(node.metadata, "creation_timestamp", None)),
                "reasons": reasons,
                "conditions": _get_conditions(node),
                "metrics": metric_detail,
            }
        )

    return {
        "cluster": kube.display_name,
        "count": len(items),
        "thresholds": {
            "node_cpu_utilization": cfg.thresholds.node_cpu_utilization,
            "node_memory_utilization": cfg.thresholds.node_memory_utilization,
        },
        "items": items,
    }


@mcp.tool(
    annotations={
        "title": "List Node Pods",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": True,
    }
)
def list_node_pods(
    node_name: Annotated[str, Field(description="节点名称")],
) -> dict[str, Any]:
    """列出指定节点上的活跃 Pod，便于 LLM 继续做资源和异常分析。"""

    kube = init_k8s_client()
    pods = _active_pods_on_node(kube.core, node_name)
    cfg = _get_runtime_config()
    items: list[dict[str, Any]] = []
    for pod in pods:
        issue = _pod_issue_summary(pod, cfg.thresholds.pod_restart_count)
        item = _pod_basic_view(pod)
        item["issue"] = issue
        items.append(item)

    return {
        "cluster": kube.display_name,
        "node": node_name,
        "count": len(items),
        "items": items,
    }


def _find_problem_container_name(pod: client.V1Pod) -> str | None:
    """尝试找出最可能出问题的容器名（用于读取日志）。"""
    for cs in pod.status.container_statuses or []:
        if cs.state and cs.state.waiting and cs.state.waiting.reason in _BAD_POD_REASONS:
            return cs.name
        if cs.last_state and cs.last_state.terminated and cs.last_state.terminated.reason in _BAD_POD_REASONS:
            return cs.name
    return None


def _read_pod_log_excerpt(
    core: client.CoreV1Api,
    namespace: str,
    pod_name: str,
    container_name: str | None,
) -> str:
    """读取 Pod 日志的短摘录，优先 previous=True 以覆盖 CrashLoopBackOff 场景。"""
    if not container_name:
        return ""

    for previous in (True, False):
        try:
            return _k8s_api_call(
                core.read_namespaced_pod_log,
                name=pod_name,
                namespace=namespace,
                container=container_name,
                previous=previous,
                tail_lines=80,
                timestamps=True,
            )[:4000]
        except ApiException:
            continue
        except Exception:
            continue
    return ""


@mcp.tool(
    annotations={
        "title": "Diagnose Pod Issues",
        "readOnlyHint": True,
        "idempotentHint": True,
        "openWorldHint": True,
    }
)
def diagnose_pod_issues(
    pod_name: Annotated[str, Field(description="Pod 名称")],
    namespace: Annotated[str, Field(description="Pod 所在命名空间")],
) -> dict[str, Any]:
    """
    对指定 Pod 做精简版诊断：
    - Pending：补充事件信息
    - CrashLoopBackOff / ImagePullBackOff 等：尝试读取 previous 日志
    """

    kube = init_k8s_client()
    cfg = _get_runtime_config()
    pod = _k8s_api_call(kube.core.read_namespaced_pod, name=pod_name, namespace=namespace)

    # issue 先识别明显异常，再识别“重启偏高”，这样输出更符合排障优先级。
    phase = pod.status.phase if pod.status else ""
    issue = _pod_issue_summary(pod, cfg.thresholds.pod_restart_count)
    problem_container = _find_problem_container_name(pod)

    # Pending/CrashLoopBackOff 等场景下，事件信息对定位非常关键，这里做采样返回。
    events = _k8s_api_call(
        kube.core.list_namespaced_event,
        namespace=namespace,
        field_selector=f"involvedObject.kind=Pod,involvedObject.name={pod_name}",
    ).items

    log_excerpt = ""
    if issue and (
        "CrashLoopBackOff" in issue
        or "ImagePullBackOff" in issue
        or "ErrImagePull" in issue
        or "OOMKilled" in issue
    ):
        log_excerpt = _read_pod_log_excerpt(kube.core, namespace, pod_name, problem_container)

    return {
        "cluster": kube.display_name,
        "namespace": namespace,
        "name": pod_name,
        "node": getattr(pod.spec, "node_name", "") or "",
        "phase": phase,
        "qos_class": _pod_qos_class(pod),
        "restart_count": _pod_restart_count(pod),
        "issue": issue,
        "problem_container": problem_container,
        "events": [_event_brief(event) for event in events[:20]],
        "log_excerpt": log_excerpt,
        "suggestions": _pod_suggestions(issue, phase),
    }


@mcp.resource("k8s://cluster/summary")
def cluster_summary() -> dict[str, Any]:
    """提供一个轻量级集群摘要，便于 LLM 在开始诊断前快速建立上下文。"""

    kube = init_k8s_client()
    cfg = _get_runtime_config()
    nodes = _k8s_api_call(kube.core.list_node).items
    problem_nodes = 0
    for node in nodes:
        reasons, _metric = _problem_reason_list(node, kube.custom, cfg)
        if reasons:
            problem_nodes += 1

    return {
        "cluster": kube.display_name,
        "node_count": len(nodes),
        "problem_node_count": problem_nodes,
        "available_tools": [
            "list_nodes",
            "get_node_detail",
            "get_node_resource_usage",
            "find_problem_nodes",
            "list_node_pods",
            "diagnose_pod_issues",
        ],
    }


@mcp.prompt
def node_triage_prompt(
    node_name: Annotated[str, Field(description="待排查的节点名称")],
) -> str:
    """给 LLM 的节点排障 SOP。"""

    return f"""
你是一名 Kubernetes SRE，请围绕节点 `{node_name}` 按以下顺序进行排障：

1. 先调用 `get_node_detail` 查看节点状态、条件、污点、基础信息和节点上的 Pod 概览。
2. 再调用 `get_node_resource_usage` 检查 allocatable、requests/limits 水位以及是否存在 BestEffort Pod 迁移建议。
3. 如果节点上有异常 Pod，再调用 `list_node_pods` 筛出异常项，并按需继续调用 `diagnose_pod_issues`。
4. 输出结论时请分别回答：
   - 节点是否真的异常，还是只是高负载/被主动 cordon；
   - 影响面主要在哪些 Pod/工作负载上；
   - 建议的下一步排查或缓解动作。
"""


@mcp.prompt
def cluster_problem_scan_prompt() -> str:
    """给 LLM 的集群问题扫描 SOP。"""

    return """
你是一名 Kubernetes 巡检助手，请按以下步骤工作：

1. 先读取 `k8s://cluster/summary` 建立整体上下文。
2. 调用 `find_problem_nodes` 找出异常节点，并按严重程度排序：
   - NodeNotReady
   - DiskPressure / MemoryPressure / PIDPressure
   - HighCPUUsage / HighMemoryUsage
3. 对每个异常节点：
   - 用 `get_node_detail` 看详细状态；
   - 用 `get_node_resource_usage` 看资源审计和 overcommit；
   - 必要时用 `list_node_pods` / `diagnose_pod_issues` 下钻。
4. 最终输出请包含：
   - 问题节点列表
   - 每个节点的关键异常原因
   - 是否需要优先处理某些 Pod/工作负载
   - 建议的处置优先级
"""


def main() -> None:
    """直接运行 MCP Server。"""
    mcp.run(transport="sse",host="0.0.0.0",port=8080)


if __name__ == "__main__":
    main()
