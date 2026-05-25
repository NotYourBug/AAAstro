"""
巡检规则模块（本项目的核心业务逻辑）。

这里实现“检查什么、怎么判断 PASS/WARN/FAIL、要输出哪些明细”。

整体约定：
- 每个检查函数返回一个 CheckResult（不会抛异常把整个巡检打崩）
- 遇到环境差异/权限不足/组件不存在等情况，倾向返回 WARN 并在 data 里给出 error/提示
- run_all(profile=...) 用于把 instruction.txt 的“执行频率”落地：
  - 高频（5m/10m）只跑关键项，降低 API 压力
  - 低频（hourly/daily）跑更全面的检查项
"""

from __future__ import annotations

import time
from typing import Any

from kubernetes import client

from .config import InspectorConfig
from .kube import apiserver_healthz, get_node_metrics, parse_quantity
from .models import CheckResult, Status


def _duration_ms(t0: float) -> int:
    """将 perf_counter 的起点转换为“耗时毫秒”。"""
    return int((time.perf_counter() - t0) * 1000)


def check_nodes(
    core: client.CoreV1Api,
    custom: client.CustomObjectsApi,
    cfg: InspectorConfig,
    include_metrics: bool = True,
) -> CheckResult:
    """
    Node 巡检：
    - Ready 状态：存在 NotReady 直接 FAIL
    - NoSchedule taint：不在允许列表则 WARN（避免业务节点被错误打上 NoSchedule）
    - 资源使用率（可选）：依赖 metrics.k8s.io，超阈值 WARN

    include_metrics=False 常用于高频 profile（例如 5m），避免频繁调用 Metrics API。
    """
    t0 = time.perf_counter()
    name = "nodes"
    try:
        nodes = core.list_node().items
        # 这里是为了获取节点列表，包括所有节点的详细信息[Node对象1, Node对象2, Node对象3]
        # 例如：节点名称、状态、条件、规格、状态、可分配资源等。
        not_ready: list[str] = []
        noschedule: list[str] = []
        capacity: dict[str, dict[str, str]] = {}  # 节点名称 -> {cpu: 节点总CPU, memory: 节点总内存}
        """
        检查节点的状态：是Ready还是NotReady
        检查节点是否有NoSchedule taint：不在允许列表则 WARN
        检查节点是否有容量信息：有则记录节点总CPU和总内存
        """

        for n in nodes:
            n_name = n.metadata.name if n.metadata else ""

            # 查状态
            ready = False  # ready先标负，后续根据node.status.conditions.type查看。
            # node.status.conditions 是一个列表，包含了节点的所有状态条件，
            # 例如：Ready、NotReady、MemoryPressure、DiskPressure、NetworkPressure、OutOfDisk、OutOfMemory、OutOfStorage等。
            for c in n.status.conditions or []:
                if c.type == "Ready":
                    ready = (c.status == "True")
                    break
            if not ready:
                not_ready.append(n_name)  # 如果说我们检查到节点的状态是NotReady，就记录NotReady节点的名称（添加到not_ready列表中）

            # 查污点
            for t in n.spec.taints or []:
                if t.effect == "NoSchedule":
                    key = t.key or ""
                    if key and key not in cfg.allow_noschedule_taints:
                        noschedule.append(f"{n_name}:{key}")   # 如果说我们检查到节点有NoSchedule taint，且不在允许列表中，就记录NoSchedule taint的名称（添加到noschedule列表中）
            
            # 查节点容量并存储，节点名:{总容量}
            if n.status and n.status.capacity:
                capacity[n_name] = {
                    "cpu": str(n.status.capacity.get("cpu", "")),
                    "memory": str(n.status.capacity.get("memory", "")),
                }

        metrics = get_node_metrics(custom) if include_metrics else {}
        util: list[dict[str, Any]] = []
        over_cpu: list[str] = []
        over_mem: list[str] = []

        if metrics:
            # 以为在kube.py中的get-node-metrics函数中的plural设置的是nodes，也就是说采集到的是node的cpu、memory的指标
            for n_name, usage in metrics.items():
                c = capacity.get(n_name)
                if not c:
                    continue
                cpu_u = parse_quantity(usage.get("cpu", "0"))
                cpu_c = parse_quantity(c.get("cpu", "0"))
                mem_u = parse_quantity(usage.get("memory", "0"))
                mem_c = parse_quantity(c.get("memory", "0"))
                """
                metric-server获取到的值，表示的是当前节点已经使用的系统资源的情况【整个节点的全部CPU/内存使用量，包含系统进程，不是只算Pod】，cpu是以毫核为基础单位，memory以字节为基础单位，这些值是动态的，随时改变。
                capacity表示的是节点的总资源容量（整机视角），这是一个相对静态的值。
                所以，我们在“节点巡检”中用 usage/capacity 来计算节点资源使用率（节点整体负载）。

                节点资源的使用率 = metrics-server拿到的实时的usage/node.status.capacity
                表示的是节点当前已经使用的资源，占“节点总资源容量”的占比

                metrics（out）：当前实际在用的CPU/内存
                c（capacity）：节点总容量
                """
                cpu_p = (cpu_u / cpu_c) if cpu_c else 0.0
                mem_p = (mem_u / mem_c) if mem_c else 0.0
                """
                这里算的利用率到底是啥：节点总CPU使用率（含系统）/节点总CPU（capacity）
                """
                util.append(
                    {
                        "node": n_name,
                        "cpu_utilization": round(cpu_p, 4),
                        "mem_utilization": round(mem_p, 4),
                    }
                )
                # 最后将计算结果放进利用率列表util中
                # 检查CPU和内存是否超过阈值
                if cpu_p >= cfg.thresholds.node_cpu_utilization:
                    over_cpu.append(n_name)   # 记录cpu资源使用超载的节点
                if mem_p >= cfg.thresholds.node_memory_utilization:
                    over_mem.append(n_name)   # 记录memory资源使用超载的节点
        
        # 存在没就绪（NotReady）的节点，就构建一个CheckResult对象，后续报错
        if not_ready:
            return CheckResult(
                name=name,
                status=Status.FAIL,
                summary=f"存在 NotReady 节点：{len(not_ready)}",
                data={"not_ready": not_ready, "noschedule_taints": noschedule, "utilization": util},
                duration_ms=_duration_ms(t0),
            )
            ### 不明白这里为什么将noschedule_taints中了，只针对not_ready的判定那就只放not_ready就得了呗。
            ### 解答：因为not_ready的优先级更大，一旦发现了not_ready那我们肯定就是先返回检查结果，但是如果存在noschedule_taints如果我们不将其加入其中那就会导致检查遗漏，所以保险起见，就将其也加入到其中了，同时也可以看到，下面的那个对noschedule的判断中没加入not_ready

        if over_cpu or over_mem:
            return CheckResult(
                name=name,
                status=Status.WARN,
                summary=f"节点资源使用率超过阈值：CPU={len(over_cpu)} Memory={len(over_mem)}",
                data={
                    "over_cpu": over_cpu,
                    "over_memory": over_mem,
                    "noschedule_taints": noschedule,
                    "utilization": util,
                    "thresholds": {
                        "cpu": cfg.thresholds.node_cpu_utilization,
                        "memory": cfg.thresholds.node_memory_utilization,
                    },
                },
                duration_ms=_duration_ms(t0),
            )

        if noschedule:
            return CheckResult(
                name=name,
                status=Status.WARN,
                summary=f"发现 NoSchedule 污点（未在允许列表）：{len(noschedule)}",
                data={"noschedule_taints": noschedule, "utilization": util},
                duration_ms=_duration_ms(t0),
            )

        status = Status.PASS   # 如果以上的所有if条件都没出发的话，那咱们就美美地PASS
        summary = f"节点 Ready：{len(nodes)}"
        if include_metrics and metrics is None:
            status = Status.WARN
            summary = "节点 Ready：正常；Metrics API 不可用（跳过 CPU/内存使用率检查）"

        return CheckResult(
            name=name,
            status=status,
            summary=summary,
            data={"count": len(nodes), "utilization": util},
            duration_ms=_duration_ms(t0),
        )
    except Exception as e:
        return CheckResult(
            name=name,
            status=Status.WARN,
            summary="节点巡检执行异常（已降级）",
            data={"error": str(e)},
            duration_ms=_duration_ms(t0),
        )


def check_pending_pvcs(core: client.CoreV1Api) -> CheckResult:
    """
    PVC 巡检：
    - 关注 Pending PVC（通常表示存储后端/StorageClass/PV 绑定存在问题）
    - 存在 Pending 直接 FAIL
    """
    t0 = time.perf_counter()
    name = "pvcs"
    try:
        pvcs = core.list_persistent_volume_claim_for_all_namespaces().items
        pending: list[dict[str, str]] = []
        for pvc in pvcs:
            phase = pvc.status.phase if pvc.status else ""
            if phase == "Pending":
                ns = pvc.metadata.namespace if pvc.metadata else ""
                n = pvc.metadata.name if pvc.metadata else ""
                pending.append({"namespace": ns, "name": n})
        if pending:
            return CheckResult(
                name=name,
                status=Status.FAIL,
                summary=f"存在 Pending PVC：{len(pending)}",
                data={"pending": pending},
                duration_ms=_duration_ms(t0),
            )
        return CheckResult(
            name=name,
            status=Status.PASS,
            summary=f"PVC 绑定正常：{len(pvcs)}",
            data={"count": len(pvcs)},
            duration_ms=_duration_ms(t0),
        )
    except Exception as e:
        return CheckResult(
            name=name,
            status=Status.WARN,
            summary="PVC 巡检执行异常（已降级）",
            data={"error": str(e)},
            duration_ms=_duration_ms(t0),
        )


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
    - str：问题描述（会被上层收集进结果 data）
    """
    if not pod.status:
        return "missing_status"
    phase = pod.status.phase or ""
    if phase in {"Failed", "Unknown"}:
        return f"phase={phase}"

    cs_list = pod.status.container_statuses or []
    # 接着来检查Pod中各容器Container的情况
    for cs in cs_list:
        # 容器状态非空，且处于等待中，并且原因在_BAD_POD_REASONS中，返回等待中原因
        if cs.state and cs.state.waiting and cs.state.waiting.reason in _BAD_POD_REASONS:
            return f"waiting={cs.state.waiting.reason}"
        # 容器状态非空，且处于终止中，并且原因在_BAD_POD_REASONS中，返回终止中原因
        if cs.last_state and cs.last_state.terminated and cs.last_state.terminated.reason in _BAD_POD_REASONS:
            return f"terminated={cs.last_state.terminated.reason}"

    if restart_threshold > 0:
        for cs in cs_list:
            if (cs.restart_count or 0) >= restart_threshold:
                return f"restart_count>={restart_threshold}"
    return None


def check_pods(core: client.CoreV1Api, cfg: InspectorConfig) -> CheckResult:
    """
    Pod 巡检：
    - phase=Failed/Unknown 或常见等待原因（CrashLoopBackOff/ImagePullBackOff 等） => FAIL
    - 重启次数超过阈值 => WARN
    """
    t0 = time.perf_counter()
    name = "pods"
    try:
        pods = core.list_pod_for_all_namespaces(watch=False).items
        bad: list[dict[str, str]] = []
        restart_heavy: list[dict[str, str]] = []
        for p in pods:
            ns = p.metadata.namespace if p.metadata else ""
            pn = p.metadata.name if p.metadata else ""
            issue = _pod_issue(p, restart_threshold=0)
            if issue:
                bad.append({"namespace": ns, "name": pn, "issue": issue})
                continue
            issue2 = _pod_issue(p, restart_threshold=cfg.thresholds.pod_restart_count)
            if issue2 == f"restart_count>={cfg.thresholds.pod_restart_count}":
                restart_heavy.append({"namespace": ns, "name": pn, "issue": issue2})

        if bad:
            return CheckResult(
                name=name,
                status=Status.FAIL,
                summary=f"发现异常 Pod：{len(bad)}",
                data={"bad": bad},
                duration_ms=_duration_ms(t0),
            )
        if restart_heavy:
            return CheckResult(
                name=name,
                status=Status.WARN,
                summary=f"Pod 重启次数偏高：{len(restart_heavy)}",
                data={"restart_heavy": restart_heavy, "restart_threshold": cfg.thresholds.pod_restart_count},
                duration_ms=_duration_ms(t0),
            )
        return CheckResult(
            name=name,
            status=Status.PASS,
            summary=f"Pod 运行正常：{len(pods)}",
            data={"count": len(pods)},
            duration_ms=_duration_ms(t0),
        )
    except Exception as e:
        return CheckResult(
            name=name,
            status=Status.WARN,
            summary="Pod 巡检执行异常（已降级）",
            data={"error": str(e)},
            duration_ms=_duration_ms(t0),
        )


def check_warning_events(core: client.CoreV1Api, cfg: InspectorConfig) -> CheckResult:
    """
    Event 巡检：
    - 拉取 type=Warning 的事件
    - 数量超过阈值则 WARN，并只采样前 200 条（避免报告过大）
    """
    t0 = time.perf_counter()
    name = "events"
    try:
        evs = core.list_event_for_all_namespaces(field_selector="type=Warning").items
        items: list[dict[str, str]] = []
        for e in evs[:200]:
            ns = e.metadata.namespace if e.metadata else ""
            n = e.involved_object.name if e.involved_object else ""
            kind = e.involved_object.kind if e.involved_object else ""
            reason = e.reason or ""
            msg = (e.message or "")[:200]
            items.append({"namespace": ns, "kind": kind, "name": n, "reason": reason, "message": msg})

        if len(evs) > cfg.thresholds.warning_events_max:
            return CheckResult(
                name=name,
                status=Status.WARN,
                summary=f"Warning Event 数量偏多：{len(evs)}（仅展示前 200 条）",
                data={"count": len(evs), "sample": items, "threshold": cfg.thresholds.warning_events_max},
                duration_ms=_duration_ms(t0),
            )
        return CheckResult(
            name=name,
            status=Status.PASS,
            summary=f"Warning Event 数量正常：{len(evs)}",
            data={"count": len(evs), "sample": items},
            duration_ms=_duration_ms(t0),
        )
    except Exception as e:
        return CheckResult(
            name=name,
            status=Status.WARN,
            summary="Event 巡检执行异常（已降级）",
            data={"error": str(e)},
            duration_ms=_duration_ms(t0),
        )


def check_control_plane(core: client.CoreV1Api, api_client: client.ApiClient, cfg: InspectorConfig) -> CheckResult:
    """
    控制平面巡检：
    - 直接请求 /healthz 判断 apiserver 是否健康、延迟是否偏高
    - 检查 kube-apiserver / kube-controller-manager / kube-scheduler 的 Pod 是否 Running

    说明：
    - 有些集群控制平面是静态 Pod/外置部署，可能查不到 Pod，这里会 WARN 而不是直接 FAIL。
    """
    t0 = time.perf_counter()
    name = "control-plane"
    ns = cfg.control_plane_namespace   # 控制平面的组件所处的命名空间
    try:
        health = apiserver_healthz(api_client, timeout_s=2.0)
        # 这就是控制平面的组件，每个组件都有一个Pod在ns命名空间下，并且都有一个component标签（可以通过kubectl describe pod来查看）
        selector_keys = [
            ("kube-apiserver", "component=kube-apiserver"),
            ("kube-controller-manager", "component=kube-controller-manager"),
            ("kube-scheduler", "component=kube-scheduler"),
        ]
        cp: dict[str, Any] = {"apiserver_healthz": health, "components": {}}  # 初始化控制平面组件的健康状态
        any_missing = False
        any_bad = False

        for comp, sel in selector_keys:
            pods = core.list_namespaced_pod(namespace=ns, label_selector=sel).items  # 拉取ns命名空间下所有component标签为sel的Pod,先检查一下这个Pod是否存在
            if not pods:
                any_missing = True      # missing标记为True，说明检查过程中出现了组件丢失的情况
                cp["components"][comp] = {"found": 0}   # 记录一下这个组件没有找到Pod，然后直接结束本次的for循环，进入下一个组件的处理
                continue
            bad: list[dict[str, str]] = []
            for p in pods:
                phase = p.status.phase if p.status else ""
                # 记录非Running状态的Pod（放进bad列表中）
                if phase != "Running":
                    bad.append(
                        {
                            "namespace": ns,
                            "name": p.metadata.name if p.metadata else "",
                            "phase": phase,
                        }
                    )
                    continue
                for cs in p.status.container_statuses or []:
                    if (cs.restart_count or 0) >= cfg.thresholds.pod_restart_count:
                        bad.append(
                            {
                                "namespace": ns,
                                "name": p.metadata.name if p.metadata else "",
                                "phase": phase,
                                "issue": f"restart_count>={cfg.thresholds.pod_restart_count}",
                            }
                        )
                        break
            if bad:
                any_bad = True
            cp["components"][comp] = {"found": len(pods), "bad": bad}  # 记录一下这个组件的Pod数量和异常Pod列表

        # 首先是如果apiserver /healthz 健康检查异常，则直接返回FAIL
        if not health.get("ok", False):
            return CheckResult(
                name=name,
                status=Status.FAIL,
                summary="apiserver /healthz 异常",
                data=cp,
                duration_ms=_duration_ms(t0),
            )
        # 经过所有组件的检查后，我们来看看any_bad这个标量是不是被标记为True了（标记为bad的Pod一种是zijibenshen就是非Running的状态，另一种是其中的某个container有些问题，那它也会被标记为bad）
        if any_bad:
            return CheckResult(
                name=name,
                status=Status.FAIL,
                summary="控制平面组件 Pod 异常",
                data=cp,
                duration_ms=_duration_ms(t0),
            )
        # 是否存在组件Pod丢失（any_missing）的情况，如果有，就返回WARN
        if any_missing:
            return CheckResult(
                name=name,
                status=Status.WARN,
                summary=f"在 {ns} 未发现部分控制平面组件 Pod（可能为静态 Pod 或外置部署）",
                data=cp,
                duration_ms=_duration_ms(t0),
            )
        # 健康检查是否超时的问题，如果延迟超过500ms，就返回WARN
        latency = health.get("latency_ms")
        if isinstance(latency, int) and latency >= 500:
            return CheckResult(
                name=name,
                status=Status.WARN,
                summary=f"apiserver /healthz 延迟偏高：{latency}ms",
                data=cp,
                duration_ms=_duration_ms(t0),
            )
        # 如果以上所有检查都通过了，那就皆大欢喜，直接返回PASS
        return CheckResult(
            name=name,
            status=Status.PASS,
            summary="控制平面健康",
            data=cp,
            duration_ms=_duration_ms(t0),
        )
    # try中如果抛出异常，就返回WARN
    except Exception as e:
        return CheckResult(
            name=name,
            status=Status.WARN,
            summary="控制平面巡检执行异常（已降级）",
            data={"namespace": ns, "error": str(e)},
            duration_ms=_duration_ms(t0),
        )


def check_kube_system_addons(core: client.CoreV1Api, cfg: InspectorConfig) -> CheckResult:
    """
    kube-system 关键组件巡检（按 label selector）。

    默认检查：
    - kube-proxy
    - calico-node（示例：若你用的是 cilium/flannel，需要改 selector）
    - coredns（示例 selector：k8s-app=kube-dns）
    """
    t0 = time.perf_counter()
    name = "kube-system-addons"
    ns = cfg.control_plane_namespace
    try:
        # 说实话，下面的套路跟上面的控制平面组件巡检是一致的，只是这里检查的是kube-system命名空间的组件组件，
        # 只是label selector不同。

        # 元组列表存放基础组件的名称和label selector，用于后续的Pod匹配
        checks = [
            ("kube-proxy", "k8s-app=kube-proxy"),
            ("cni-calico", "k8s-app=calico-node"),
            ("coredns", "k8s-app=kube-dns"),
        ]
        out: dict[str, Any] = {"namespace": ns, "addons": {}}
        any_bad = False
        any_missing = False

        for addon, selector in checks:
            pods = core.list_namespaced_pod(namespace=ns, label_selector=selector).items
            if not pods:
                any_missing = True
                out["addons"][addon] = {"found": 0}
                continue
            bad: list[dict[str, str]] = []
            for p in pods:
                phase = p.status.phase if p.status else ""
                if phase != "Running":
                    bad.append({"name": p.metadata.name if p.metadata else "", "phase": phase})
            if bad:
                any_bad = True
                out["addons"][addon] = {"found": len(pods), "bad": bad}

        if any_bad:
            return CheckResult(
                name=name,
                status=Status.WARN,
                summary="kube-system 关键组件存在异常 Pod",
                data=out,
                duration_ms=_duration_ms(t0),
            )
        if any_missing:
            return CheckResult(
                name=name,
                status=Status.WARN,
                summary="kube-system 未发现部分关键组件（按实际 CNI/DNS 适配）",
                data=out,
                duration_ms=_duration_ms(t0),
            )
        return CheckResult(
            name=name,
            status=Status.PASS,
            summary="kube-system 关键组件健康",
            data=out,
            duration_ms=_duration_ms(t0),
        )
    except Exception as e:
        return CheckResult(
            name=name,
            status=Status.WARN,
            summary="kube-system 组件巡检执行异常（已降级）",
            data={"namespace": ns, "error": str(e)},
            duration_ms=_duration_ms(t0),
        )

# 终于来到了咱们的超级核心函数，根据profile选择并执行巡检项
def run_all(
    core: client.CoreV1Api,
    storage: client.StorageV1Api,
    custom: client.CustomObjectsApi,
    api_client: client.ApiClient,
    cfg: InspectorConfig,
    profile: str = "full",
) -> list[CheckResult]:
    """
    根据 profile 选择并执行巡检项。

    profile 设计是为了落地 instruction.txt 的“执行频率”：
    - 5m：数据平面关键项（更轻量，避免 API 压力）
    - 10m：控制平面健康
    - hourly：基础设施关键项
    - daily/full：全量巡检
    """
    p = (profile or "full").strip().lower()
    if p in {"5m", "dataplane-5m", "dataplane"}:
        return [
            check_nodes(core, custom, cfg, include_metrics=False),
            check_pods(core, cfg),
            check_pending_pvcs(core),
        ]
    if p in {"10m", "controlplane-10m", "controlplane"}:
        return [check_control_plane(core, api_client, cfg)]
    if p in {"hourly", "1h"}:
        return [
            check_control_plane(core, api_client, cfg),
            check_nodes(core, custom, cfg, include_metrics=True),
            check_pods(core, cfg),
            check_pending_pvcs(core),
        ]
    if p in {"daily", "1d", "full"}:
        return [
            check_control_plane(core, api_client, cfg),
            check_nodes(core, custom, cfg, include_metrics=True),
            check_pods(core, cfg),
            check_pending_pvcs(core),
            check_warning_events(core, cfg),
            check_kube_system_addons(core, cfg),
        ]
    return [
        CheckResult(
            name="profile",
            status=Status.WARN,
            summary=f"未知 profile：{profile}，已降级为 full",
            data={"profile": profile},
            duration_ms=0,
        ),
        check_control_plane(core, api_client, cfg),
        check_nodes(core, custom, cfg, include_metrics=True),
        check_pods(core, cfg),
        check_pending_pvcs(core),
        check_warning_events(core, cfg),
        check_kube_system_addons(core, cfg),
    ]
