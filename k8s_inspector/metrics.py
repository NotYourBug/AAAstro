"""
Prometheus 指标导出模块（面向批处理巡检任务）。

这个项目的巡检脚本通常以 CronJob 形式运行：脚本启动→执行巡检→退出。
Prometheus 的常见采集方式是“Prometheus 定时 scrape 一个长期存活的 /metrics HTTP 服务”，
但对短生命周期的 CronJob 并不友好。

因此这里采用 Pushgateway 模式：
- 巡检脚本执行完成后，把本次 Report 转换成 Prometheus 指标；
    - 时序数据的标准格式：指标名{标签值} 值 时间戳
    - 例如：k8s_inspector_last_run_timestamp{cluster="k8s-1",profile="5m"} 1694502400 1694502400
- 推送到 Pushgateway（push_to_gateway）；
- Prometheus 再去 scrape Pushgateway；
- Grafana/Alertmanager 使用这些指标进行展示/告警。

本模块只负责“Report -> Metrics -> Pushgateway”，不负责告警路由与通知。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .models import Report, Status


@dataclass
class PushGatewayConfig:
    """Pushgateway 推送配置。"""
    url: str
    job: str = "k8s-inspector"
    timeout_s: float = 5.0



"""
1. 拿到一份巡检报告
2. 创建一个干净的指标注册表     
3. 把报告里的所有检查结果 → 变成 Prometheus 指标
4. 给这次推送打上唯一标识（cluster + profile）
5. 推送到 PushGateway
6. Prometheus 从 PushGateway 拉取指标
"""
def push_report_to_gateway(
    report: Report,
    profile: str,
    cfg: PushGatewayConfig,
    extra_grouping: dict[str, str] | None = None,
) -> None:
    """
    将一次巡检报告推送到 Pushgateway。

    - report：巡检结果（models.Report）
    - profile：执行频率档位（5m/10m/hourly/daily/full）
    - cfg：Pushgateway 地址与 job 名
    - extra_grouping：额外的 grouping_key（会与 cluster/profile 合并），用于区分不同实例/任务
    """
    prometheus_client = _import_prometheus_client()
    """
    这一种导入函数库的方法从来没有见到过，是一种延迟导入prometheus_client的方法
      - 不是程序启动就导入prometheus_client，而是在需要时才导入
      - 减少依赖加载时间，避免启动报错
    """
    registry = prometheus_client.CollectorRegistry()  # 创建独立的注册表
    """
    让每个实例/任务都有一个独立的注册表，避免指标冲突
    这样可以确保每个实例/任务的指标是独立的，不会被其他实例/任务干扰，不会和全局指标混在一起
    推送完后，注册表会被自动清空，不会影响其他实例/任务的指标推送
    """
    populate_registry(report=report, profile=profile, registry=registry)  # 这个函数就是把巡检报告 --> 转成 Prometheus 指标的
    grouping_key = dict(extra_grouping or {})
    grouping_key.setdefault("cluster", report.cluster)
    grouping_key.setdefault("profile", profile)
    """
    构建 grouping_key，用于区分不同实例/任务
    grouping_key是PushGateway用来区分“谁推的指标”的唯一标识。
    例如：{"cluster":"k8s-1","profile":"5m"}
    如果不设置的话，后推的会覆盖先推的。
    这里固定带上两个维度：
      - cluster：Kubernetes 集群名称
      - profile：执行频率档位
    保证：
      - 不同集群 --> 不覆盖
      - 不同执行频率 --> 不覆盖
      - 额外标签 --> 不覆盖
    """
    # 接着就是把指标推送到 Pushgateway
    prometheus_client.push_to_gateway(
        gateway=cfg.url,
        job=cfg.job,
        registry=registry,
        grouping_key=grouping_key,
        timeout=cfg.timeout_s,
    )
    """
    - gateway：PushGateway 地址
    - job：任务名（比如 inspector）
    - registry：本次要推送的所有指标
    - grouping_key：唯一标识，防止覆盖
    - timeout：推送超时时间（默认 5秒）
    """


def populate_registry(report: Report, profile: str, registry: Any) -> None:
    """
    把 Report 填充到 Prometheus CollectorRegistry。

    该函数纯内存操作，不做网络操作，便于单元测试。

    整体作用：
      从巡检报告 Report 中读取数据，创建6个Prometheus Gauge指标，把所有巡检结果填进去，存入registry等待推送。
        - k8s_inspector_last_run_timestamp：上次运行时间戳，单位秒
        - k8s_inspector_report_summary：报告摘要，包含 total/pass/warn/fail/skip
        - k8s_inspector_check_status：检查状态，0=PASS,1=WARN,2=FAIL,3=SKIP
        - k8s_inspector_check_duration_ms：检查执行时间，单位毫秒
        - k8s_inspector_check_issue_count：检查问题计数
        - k8s_inspector_node_utilization：节点利用率，单位比值
    
    """
    prometheus_client = _import_prometheus_client()

    """
    定义Gauge指标定义函数中的参数都是啥意思呢：
     - name：指标名称，全局唯一
     - help：指标描述，写清楚这个指标是干啥的，用于在 UI 中显示
     - labelnames：指标标签，给指标加维度的，用于在 UI 中显示，区分不同场景（哪个集群、哪种执行频率、哪个检查项、哪个节点）
     - registry：指标注册表，用于存储指标（指定注册到哪个表里）
    """
    g_last_run = prometheus_client.Gauge(
        "k8s_inspector_last_run_timestamp",
        "Last run time of k8s-inspector (unix epoch seconds).",
        labelnames=["cluster", "profile"],
        registry=registry,
    )  # 记录巡检最后执行时间戳
    g_summary = prometheus_client.Gauge(
        "k8s_inspector_report_summary",
        "Summary counters of k8s-inspector report.",
        labelnames=["cluster", "profile", "status"],
        registry=registry,
    )  # 报告总览（总数/通过数/警告数/失败数/跳过数），多了一个status标签（total/pass/warn/fail/skip）
    g_check_status = prometheus_client.Gauge(
        "k8s_inspector_check_status",
        "Check status (0=PASS,1=WARN,2=FAIL,3=SKIP).",
        labelnames=["cluster", "profile", "check"],
        registry=registry,
    )  # 每个检查项的最终状态，0=PASS,1=WARN,2=FAIL,3=SKIP，check=检查项名称
    g_check_duration = prometheus_client.Gauge(
        "k8s_inspector_check_duration_ms",
        "Check execution duration in milliseconds.",
        labelnames=["cluster", "profile", "check"],
        registry=registry,
    )  # 检查执行时间，单位毫秒，check=检查项名称
    g_issue = prometheus_client.Gauge(
        "k8s_inspector_check_issue_count",
        "Issue counters extracted from check data.",
        labelnames=["cluster", "profile", "check", "issue"],
        registry=registry,
    )  # 每个检查项的问题计数，check=检查项名称，issue=问题类型
    g_node_util = prometheus_client.Gauge(
        "k8s_inspector_node_utilization",
        "Node utilization (ratio).",
        labelnames=["cluster", "profile", "node", "resource"],
        registry=registry,
    )  # 节点利用率，单位比值，node=节点名称，resource=资源类型（cpu/memory）

    ts = _parse_iso_to_epoch(report.generated_at)  # 把报告生成时间转成时间戳
    g_last_run.labels(report.cluster, profile).set(ts)  # 记录巡检最后执行时间戳（设置这个指标对应的指标值）

    summary = report.to_dict().get("summary", {})  # 每个Status一种有多少个
    for k in ["total", "pass", "warn", "fail", "skip"]:
        v = summary.get(k)
        if isinstance(v, int):
            g_summary.labels(report.cluster, profile, k).set(v)
            """示例：
            k8s_inspector_report_summary{status="pass"} 👉 28
            k8s_inspector_report_summary{status="fail"} 👉 2
            """
    # 遍历所有检查项结果，Report中的results对应的是一个CheckResult列表
    for r in report.results:
        g_check_status.labels(report.cluster, profile, r.name).set(_status_to_number(r.status))
        if r.duration_ms is not None:  # 如果说检查项执行时间不为空，那么同时也记录下检查项执行时间
            g_check_duration.labels(report.cluster, profile, r.name).set(r.duration_ms)

        for issue_name, count in _extract_issue_counts(r.name, r.data).items():
            g_issue.labels(report.cluster, profile, r.name, issue_name).set(count)

        if r.name == "nodes":  # 如果说检查项（CheckResult）是nodes，那么同时也记录下节点利用率
            for it in _extract_node_utilization(r.data):
                g_node_util.labels(
                    report.cluster,
                    profile,
                    it["node"],
                    it["resource"],
                ).set(it["value"])

"""
提取问题数量函数：_extract_issue_counts，最终返回的结果是一个字典，键是问题类型（issue_name），值是问题数量（count）。
"""
def _extract_issue_counts(check_name: str, data: dict[str, Any]) -> dict[str, int]:
    """从检查项 data 中提取“计数型问题”，用于指标化（例如 pending pvc 数量）。"""
    out: dict[str, int] = {}
    if not isinstance(data, dict):
        return out

    if check_name == "nodes":
        out["not_ready"] = len(data.get("not_ready") or [])
        out["noschedule_taints"] = len(data.get("noschedule_taints") or [])
        out["over_cpu"] = len(data.get("over_cpu") or [])
        out["over_memory"] = len(data.get("over_memory") or [])
        # 如果检查项是nodes的话，可能出现的错误情况就是上面那些，我们在checks.py程序中也写了对应的检查逻辑代码。
    elif check_name == "pods":
        out["bad"] = len(data.get("bad") or [])
        out["restart_heavy"] = len(data.get("restart_heavy") or [])
    elif check_name == "pvcs":
        out["pending"] = len(data.get("pending") or [])
    elif check_name == "events":
        c = data.get("count")
        if isinstance(c, int):
            out["warning"] = c
    elif check_name == "control-plane":
        health = data.get("apiserver_healthz") or {}
        ok = health.get("ok")
        if ok is False:
            out["healthz_not_ok"] = 1
    elif check_name == "kube-system-addons":
        addons = (data.get("addons") or {}) if isinstance(data.get("addons"), dict) else {}
        missing = 0
        bad = 0
        for v in addons.values():
            if not isinstance(v, dict):
                continue
            found = v.get("found")
            if found == 0:
                missing += 1
            b = v.get("bad") or []
            if isinstance(b, list) and b:
                bad += 1
        out["addons_missing"] = missing
        out["addons_bad"] = bad
    return {k: int(v) for k, v in out.items() if isinstance(v, int)}
    # 返回的是一个字典，键是问题类型（issue_name），值是问题数量（count）。


def _extract_node_utilization(data: dict[str, Any]) -> list[dict[str, Any]]:
    """从 nodes 检查项 data 中提取每节点 cpu/memory utilization（ratio）。"""
    util = data.get("utilization") if isinstance(data, dict) else None  
    if not isinstance(util, list):   # 如果说util不是列表，那么直接返回空列表（这个util正常情况下应该是一个字典列表，其中包括集群中的所有节点及其对应的系统资源使用情况）
        return []
    out: list[dict[str, Any]] = []
    for it in util:
        if not isinstance(it, dict):
            continue
        node = it.get("node")
        cpu = it.get("cpu_utilization")
        mem = it.get("mem_utilization")
        if isinstance(node, str) and isinstance(cpu, (int, float)):
            out.append({"node": node, "resource": "cpu", "value": float(cpu)})
        if isinstance(node, str) and isinstance(mem, (int, float)):
            out.append({"node": node, "resource": "memory", "value": float(mem)})
        """
        我本来在想既然util已经是一个字典了，其中包含了节点的名称，以及资源名字和其对应的使用情况，那么直接返回这个util不就行了吗，
        但是这里突然想到，util中的资源全都和到了一起，那么我需要将cpu_utilization和mem_utilization分别提取出来，才能用于指标化。
        所以，我需要将util中的每个字典都遍历一次，提取出node、cpu_utilization和mem_utilization。
        """
    return out

# 传入report的generate_at字段，将其转化为时间戳（单位：秒）
# 例如："2023-08-01T12:34:59:599Z" -> 1796000000.0
def _parse_iso_to_epoch(s: str) -> float:
    """将 ISO8601 时间转换为 unix epoch 秒；失败返回 0。"""
    try:
        dt = datetime.fromisoformat(s)
        return dt.timestamp()
    except Exception:
        return 0.0


def _status_to_number(s: Status) -> int:
    """将 PASS/WARN/FAIL/SKIP 映射为 0/1/2/3，便于在 Grafana/规则里比较。"""
    if s == Status.PASS:
        return 0
    if s == Status.WARN:
        return 1
    if s == Status.FAIL:
        return 2
    return 3


def _import_prometheus_client():
    """按需导入 prometheus_client，避免未安装时影响核心巡检功能。"""
    try:
        import prometheus_client  # type: ignore
        from prometheus_client.exposition import push_to_gateway  # type: ignore

        prometheus_client.push_to_gateway = push_to_gateway  # type: ignore[attr-defined]
        return prometheus_client
    except Exception as e:  # pragma: no cover
        raise RuntimeError("需要安装 prometheus-client 才能导出/推送指标") from e

"""
经过上面的代码，我们最终可以参数如下的时序数据：
- k8s_inspector_last_run_timestamp{cluster="prod",profile="5m"} 174xxxxxx
- k8s_inspector_report_summary{cluster="prod",profile="5m",status="pass"} 28
- k8s_inspector_report_summary{cluster="prod",profile="5m",status="fail"} 2
- k8s_inspector_check_status{cluster="prod",profile="5m",check="nodes"} 0
- k8s_inspector_check_duration_ms{cluster="prod",profile="5m",check="nodes"} 420
- k8s_inspector_check_issue_count{cluster="prod",profile="5m",check="pods",issue="OOMKilled"} 3
- k8s_inspector_node_utilization{cluster="prod",profile="5m",node="node-1",resource="cpu"} 0.78
"""
