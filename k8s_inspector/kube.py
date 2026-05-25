"""
Kubernetes 访问层（Kubernetes SDK 的薄封装）。

为什么需要这一层：
- CLI/巡检逻辑不应该关心“如何 load kubeconfig / in-cluster config”
- 一些特殊 API（例如 /healthz、metrics.k8s.io）调用细节适合集中在这里
- 对外提供稳定的函数：load_kube、apiserver_healthz、get_node_metrics、parse_quantity

本模块不包含具体巡检规则；巡检规则在 checks.py。
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from kubernetes import client, config
from kubernetes.client import ApiClient
from kubernetes.client.exceptions import ApiException

# dataclass是用来快速创建数据类的，它自动为数据类添加 __init__ 方法、__repr__ 方法等
# 例如：KubeContext 类自动添加 __init__ 方法，用于初始化 display_name、api_client、core、storage、custom 字段
# 用这个方法可以少写些重复的代码。
@dataclass
class KubeContext:
    """
    一次巡检所需的 K8s 客户端集合。

    display_name:
      - 用于报告展示：in-cluster / kubeconfig / 指定 context 名
    """
    display_name: str
    api_client: ApiClient
    core: client.CoreV1Api
    storage: client.StorageV1Api
    custom: client.CustomObjectsApi


def load_kube(
    kubeconfig: str | None = None,
    context: str | None = None,
    in_cluster: bool = False,
) -> KubeContext:
    """
    初始化 Kubernetes SDK 的配置并返回 API 客户端集合。

    - in_cluster=True：用于运行在集群内部（Pod/CronJob），读取 ServiceAccount token
    - in_cluster=False：用于本地运行（开发/手工巡检），读取 kubeconfig
    """
    if in_cluster:
        config.load_incluster_config()
        display = "in-cluster"
        """
        incluster=True：程序运行在K8s内部（Pod里）--> 自动读取ServiceAccount权限
        incluster=False：程序运行在本地（开发/手工巡检）--> 读取kubeconfig（~/.kube/config）
        """
    else:
        config.load_kube_config(config_file=kubeconfig, context=context)
        display = context or "kubeconfig"


    api_client = client.ApiClient()  # 初始化 API 客户端
    """
    看到这里我是有点疑义的，大多是的情况下我们调用k8s的API，都是说直接通过client对象就调用了，没见到开始先初始化一个api_client对象。
    就比如说如果要操作Pod之类的，就直接core = client.CoreV1Api()，括号里啥都不用加的。

    1. 我们平时写的CoreV1Api()，是同步调用的，不需要等待响应，会自动创建一个默认客户端。
    2. 但这里手动创建api_client，是为了：
        1. 所有的客户端（core、storage、custom）都需要使用同一个api_client对象，共用同一个连接。
        2. 方便统一设置超时、重试、header等参数。
        3. 代码更规范、更易维护
    3. 不是异步！不是必须！只是更规范！
    可以理解为: api_client=一根网线，所有的客户端都是用这跟网线访问K8s。
    """
    return KubeContext(
        display_name=display,
        api_client=api_client,
        core=client.CoreV1Api(api_client),
        storage=client.StorageV1Api(api_client),
        custom=client.CustomObjectsApi(api_client),
    )


def apiserver_healthz(api_client: ApiClient, timeout_s: float = 2.0) -> dict[str, Any]:
    """
    调用 apiserver 的 /healthz，检查APIServer活没活，就像curl http://k8s-api:6443/healthz。

    返回字段（是否健康、耗时、状态码）：
    - ok: bool（status=200 且 body=ok）
    - status_code: HTTP 状态码

    - latency_ms: 请求耗时（毫秒）
    - body/error: 截断后的响应体/错误信息
    """
    t0 = time.perf_counter()
    try:
        data, status_code, _headers = api_client.call_api(
            "/healthz",
            "GET",
            response_type="str",
            auth_settings=["BearerToken"],
            _request_timeout=timeout_s,
            _preload_content=True,
        )
        """
        通过K8s底层客户端，手动发送一个HTTP GET请求到APIServer的/healthz接口，检查集群是否健康。
        等价于curl https://你的APIServer:6443/healthz，这个接口是k8s自带的健康检查接口
        call_api会返回三个值：
        1. 响应体（data），接口返回的内容，/healthz正常返回"ok"
        2. 状态码（status_code），200表示健康，500表示挂了，超时表示报错
        3. 响应头（_headers），HTTP响应头（这里用不到，所以加了下划线表示忽略）

        接口 + HTTP方法
        response_type：告诉客户端返回值是字符串
        auth_settings：使用Bearer Token进行认证（ServiceAccount Token或kubeconfig里的证书认证）
            活交给load_kube自动搞定。
        _request_timeout：设置超时时间（秒）
        _preload_content：是否预加载响应体（True表示预加载，False表示不预加载），表示立即读取响应内容，让data直接拿到字符串。
        """
        latency_ms = int((time.perf_counter() - t0) * 1000)
        ok = (status_code == 200) and (str(data).strip().lower() == "ok")
        return {
            "ok": ok,
            "status_code": status_code,
            "latency_ms": latency_ms,
            "body": (str(data).strip()[:200] if data is not None else ""),
        }
    except ApiException as e:
        latency_ms = int((time.perf_counter() - t0) * 1000)
        return {
            "ok": False,
            "status_code": getattr(e, "status", None),
            "latency_ms": latency_ms,
            "error": str(e),
        }
    except Exception as e:
        latency_ms = int((time.perf_counter() - t0) * 1000)
        return {"ok": False, "latency_ms": latency_ms, "error": str(e)}

# 十进制单位，CPU用（以秒为标准）
_DECIMAL = {
    "n": 1e-9,  # 纳
    "u": 1e-6,  # 微
    "m": 1e-3,  # 毫 → 1m = 0.001
    "": 1.0,    # 没有单位 = 1倍
    "k": 1e3,   # 千
    "M": 1e6,   # 兆
    "G": 1e9,   # 吉
    "T": 1e12,
    "P": 1e15,
    "E": 1e18,
}
# 500m = 500 x 1e-3 = 0.5核

# 二进制单位（内存/存储用），以字节为标准。
_BINARY = {
    "Ki": 1024.0,     # 1024字节，KB
    "Mi": 1024.0**2,  # 1024KB，MB
    "Gi": 1024.0**3,  # 1024MB，GB
    "Ti": 1024.0**4,  # 1024GB，TB
    "Pi": 1024.0**5,  # 1024TB，PB
    "Ei": 1024.0**6,  # 1024PB，EB
}
# 1Gi = 1 x 1024 x 1024 x 1024 byte = 1073741824 byte
"""
这里设置的这些是为了把K8s里的资源单位字符串，转换成计算机能计算的数字
K8s里的资源长这样：
- cpu: "250m"
- memory: "128Mi"
- 存储: "10Gi"
这个人可以看懂，但是程序就不晓得该怎么计算了，所以必须写一个函数把它们变成计算机能计算的数字：
- cpu: "250m" -> 0.25
- memory: "128Mi" -> 134217728 byte
- 存储: "10Gi" -> 1073741824 byte
这个函数就是下面的parse_quantity
"""

def parse_quantity(q: str) -> float:
    """
    解析 K8s resource quantity 字符串为数值。
    传进来的是单个数据，这就意味着在使用的是有需要我们对需要处理的数据逐一调用此函数，也有好处，就是拿到一个处理一个，不用说等都拿到了再汇总处理。
    还有就是传进来字符串，最后获得的是浮点数，方便后续系统的识别和计算。

    示例：
    - cpu: "250m" -> 0.25
    - memory: "128Mi" -> 134217728

    返回值单位：
    - cpu：以“核”为基准（m 表示 1e-3）
    - memory：以“字节”为基准（Ki/Mi/Gi 等为 1024 进位）
    """
    s = (q or "").strip()  # 把传入的字符串去空格，如果传进来的是空，就变成空字符串。
    # 空字符串直接返回0
    if not s:
        return 0.0
    # 先检查二进制单位（内存）
    for suf, mult in _BINARY.items():
        # endswith函数是用来检查一个字符串是不是以一个指定的后缀结尾的
        if s.endswith(suf):
            return float(s[: -len(suf)]) * mult
            # 去掉单位，只保留数字部分，然后再进行转换运算
    for suf, mult in _DECIMAL.items():
        if suf and s.endswith(suf):
            return float(s[: -len(suf)]) * mult
    return float(s)

# 获取k8s集群里所有节点的实时CPU/内存使用率（前提是安装了metrics-server）
def get_node_metrics(
    custom: client.CustomObjectsApi,
) -> dict[str, dict[str, str]] | None:
    """
    读取 metrics.k8s.io 的 Node 指标。
    custom是k8s自定义资源客户端（前面KubeContext里进行过封装）

    - 若集群未安装 metrics-server 或无权限，返回 None（上层应降级处理）
    - 返回格式：
      {
        "node-1": {"cpu": "123m", "memory": "456Mi"},
        ...
      }
      dict[str,dict[str,str]]这种形式
    """
    try:
        obj = custom.list_cluster_custom_object(
            group="metrics.k8s.io",
            version="v1beta1",
            plural="nodes",
        )
        """
        metrics.k8s.io表示属于监控API组
        v1beta1表示版本
        plural="nodes"表示要查的是节点指标
        """
        # 如果出现了没装metrics-server或没权限以及APIServer挂了的情况，直接返回None，不崩溃。
    except Exception:
        return None

    """
    判断返回结果是不是合法字典
    判断里面有没有items（节点列表）
    判断items是不是合法的字典列表
    """
    items = obj.get("items") if isinstance(obj, dict) else None
    if not isinstance(items, list):
        return None

    out: dict[str, dict[str, str]] = {}
    for it in items:
        md = it.get("metadata", {}) if isinstance(it, dict) else {}
        name = md.get("name")
        usage = it.get("usage", {})
        """
        我们只要元数据+节点名称+节点资源情况
        """
        if isinstance(name, str) and isinstance(usage, dict):
            out[name] = {
                "cpu": str(usage.get("cpu", "")),
                "memory": str(usage.get("memory", "")),
            }
    return out
