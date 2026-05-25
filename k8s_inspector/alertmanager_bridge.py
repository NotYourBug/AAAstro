"""
Alertmanager -> 飞书 的 Webhook 桥接服务。

这个文件解决两个问题：
1) Alertmanager 的 webhook payload 是结构化 JSON，需要解析/格式化；
2) 你希望“告警自动拉群、拉人、解决后改群名”，这需要飞书应用机器人能力与状态持久化。

它支持两种模式（通过 FEISHU_MODE 或 --mode 选择）：
- webhook 模式：
  - 使用飞书“群自定义机器人 webhook”发送文本到固定群（最简单）。
  - 只负责“发消息”，无法创建群/改群名。
- app 模式（推荐满足你需求的模式）：
  - 使用飞书“企业自建应用机器人”（lark-oapi）：
    - 第一次 FIRING：创建群 -> 拉人 -> 发告警卡片
    - RESOLVED：修改群名追加后缀（默认 [已解决]）并发送恢复卡片
  - 使用 JsonStateStore 记录 group_key -> chat_id 映射，避免重复拉群。

运行方式（示例）：
  python -m k8s_inspector.alertmanager_bridge --mode app --listen-port 8000

配套的 Kubernetes 部署文件：deploy/alertmanager-feishu-bridge.yaml。
"""

from __future__ import annotations

import argparse
import json
import os
import traceback
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer         # 构筑WebHook服务器
from typing import Any

from .feishu_app import FeishuAppClient, build_alert_card, load_feishu_app_config_from_env
from .notify import send_feishu_text
from .state_store import ChatRecord, JsonStateStore


@dataclass
class BridgeConfig:
    """桥接服务配置（从 CLI 参数与环境变量合并得出）。"""
    mode: str                              # 飞书推送模式：webhook=群机器人；app=企业自建应用机器人（支持拉群/改群名）
    feishu_webhook: str | None             # 飞书群机器人 webhook（也可用环境变量 FEISHU_WEBHOOK）
    feishu_chat_name_template: str         # 群名模板（app 模式），可用变量：{cluster} {alertname} {profile} {check}
    feishu_chat_resolved_suffix: str       # 解决后群名后缀（app 模式），默认 [已解决]
    feishu_alert_user_open_ids: list[str]  # 告警拉群成员 open_id 列表（逗号分隔；也可用环境变量 FEISHU_ALERT_USER_OPEN_IDS）
    state_path: str                        # 状态文件路径（group_key->chat_id 映射；也可用环境变量 FEISHU_CHAT_STATE_PATH）
    report_dir: str                        # 巡检报告根目录（用于从 report.json 提取更可读的 summary/data）
    report_max_results: int = 5            # 最多展示多少个 FAIL/WARN 的检查项
    report_max_items: int = 3              # 每个检查项 data 中最多展示多少个条目
    listen_host: str = "0.0.0.0"           # 监听主机（默认 0.0.0.0）
    listen_port: int = 8000                # 监听端口（默认 8000）
    timeout_s: float = 5.0                 # 请求超时时间（秒）（默认 5.0）


def build_parser() -> argparse.ArgumentParser:
    """定义命令行参数。部署到 K8s 时通常用环境变量配置。"""
    p = argparse.ArgumentParser(prog="k8s-inspector-alert-bridge")
    p.add_argument("--listen-host", default="0.0.0.0")
    p.add_argument("--listen-port", default=8000, type=int)
    p.add_argument(
        "--report-dir",
        default=None,
        help="巡检报告根目录（默认 /data/out；用于从 latest report.json 提取更多明细）",
    )
    p.add_argument("--report-max-results", default=5, type=int)
    p.add_argument("--report-max-items", default=3, type=int)
    p.add_argument(
        "--feishu-webhook",
        default=None,
        help="飞书群机器人 webhook（也可用环境变量 FEISHU_WEBHOOK）",
    )
    p.add_argument(
        "--mode",
        default=None,
        choices=["webhook", "app"],
        help="飞书推送模式：webhook=群机器人；app=企业自建应用机器人（支持拉群/改群名）",
    )
    p.add_argument(
        "--chat-name-template",
        default=None,
        help="群名模板（app 模式），可用变量：{cluster} {alertname} {profile} {check}",
    )
    p.add_argument(
        "--resolved-suffix",
        default=None,
        help="解决后群名后缀（app 模式），默认 [已解决]",
    )
    p.add_argument(
        "--alert-user-open-ids",
        default=None,
        help="告警拉群成员 open_id 列表（逗号分隔；也可用环境变量 FEISHU_ALERT_USER_OPEN_IDS）",
    )
    p.add_argument(
        "--state-path",
        default=None,
        help="状态文件路径（group_key->chat_id 映射；也可用环境变量 FEISHU_CHAT_STATE_PATH）",
    )
    p.add_argument("--timeout", default=5.0, type=float)
    return p


def alertmanager_payload_to_text(payload: dict[str, Any]) -> str:
    """将 Alertmanager webhook payload 格式化为纯文本（用于 webhook 模式推送）。"""
    status = str(payload.get("status", "")).upper() or "UNKNOWN"
    alerts = payload.get("alerts")
    if not isinstance(alerts, list):
        alerts = []

    firing = [a for a in alerts if isinstance(a, dict) and a.get("status") == "firing"]
    resolved = [a for a in alerts if isinstance(a, dict) and a.get("status") == "resolved"]

    lines: list[str] = [f"Alertmanager: {status}"]
    if firing:
        lines.append(f"FIRING: {len(firing)}")

        lines.extend(_format_alerts(firing, limit=12))
    if resolved:
        lines.append(f"RESOLVED: {len(resolved)}")
        lines.extend(_format_alerts(resolved, limit=12))
    return "\n".join(lines)


def _format_alerts(alerts: list[dict[str, Any]], limit: int) -> list[str]:
    out: list[str] = []
    for a in alerts[:limit]:
        labels = a.get("labels") if isinstance(a.get("labels"), dict) else {}
        ann = a.get("annotations") if isinstance(a.get("annotations"), dict) else {}
        name = labels.get("alertname") or "Alert"
        cluster = labels.get("cluster")
        profile = labels.get("profile")
        check = labels.get("check")
        sev = labels.get("severity")
        summary = ann.get("summary") or ann.get("message") or ""
        parts = [str(name)]
        if sev:
            parts.append(f"severity={sev}")
        if cluster:
            parts.append(f"cluster={cluster}")
        if profile:
            parts.append(f"profile={profile}")
        if check:
            parts.append(f"check={check}")
        header = " | ".join(parts)
        if summary:
            out.append(f"- {header}: {summary}")
        else:
            out.append(f"- {header}")
    if len(alerts) > limit:
        out.append(f"- ... and {len(alerts) - limit} more")
    return out


def run_server(cfg: BridgeConfig) -> None:
    """
    启动一个简单 HTTP 服务，接收 Alertmanager webhook。

    约定：
    - 接收路径：POST /alert（或 /）
    - 返回：200 ok / 500 error
    """
    store = JsonStateStore(cfg.state_path)
    feishu_app: FeishuAppClient | None = None
    if cfg.mode == "app":
        feishu_app = FeishuAppClient(load_feishu_app_config_from_env())
    print(
        f"alert-bridge starting: mode={cfg.mode}, listen={cfg.listen_host}:{cfg.listen_port}, state_path={cfg.state_path}",
        flush=True,
    )

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            if self.path not in {"/", "/alert"}:
                self.send_response(404)
                self.end_headers()
                return
            length = int(self.headers.get("Content-Length", "0") or "0")
            raw = self.rfile.read(length) if length > 0 else b""  # 读取Altermanager发送来的POST请求体（payload）
            try:
                payload = json.loads(raw.decode("utf-8", errors="replace")) if raw else {}
                if not isinstance(payload, dict):
                    payload = {}
                gk = payload.get("groupKey")
                status = payload.get("status")
                alerts = payload.get("alerts")
                alerts_n = len(alerts) if isinstance(alerts, list) else 0
                print(
                    f"alert-bridge recv: path={self.path} status={status} groupKey={gk} alerts={alerts_n}",
                    flush=True,
                )

                if cfg.mode == "webhook":
                    if not cfg.feishu_webhook:
                        raise RuntimeError("缺少 FEISHU_WEBHOOK")
                    text = alertmanager_payload_to_text(payload)
                    alerts_list = alerts if isinstance(alerts, list) else []
                    firing = [a for a in alerts_list if isinstance(a, dict) and a.get("status") == "firing"]
                    resolved = [a for a in alerts_list if isinstance(a, dict) and a.get("status") == "resolved"]
                    labels = _pick_labels(firing or resolved)
                    detail = _build_report_detail_lines(
                        report_dir=cfg.report_dir,
                        profile=labels.get("profile", ""),
                        check_hint=labels.get("check", ""),
                        max_results=cfg.report_max_results,
                        max_items=cfg.report_max_items,
                    )
                    if detail:
                        text = text + "\n----\n" + "\n".join(detail)
                    send_feishu_text(cfg.feishu_webhook, text=text, timeout_s=cfg.timeout_s)
                    """
                    如果是 webhook 模式，直接发送文本消息
                    """
                else:
                    if not feishu_app:
                        raise RuntimeError("飞书应用机器人未初始化")
                    _handle_alert_payload_app_mode(
                        payload=payload,
                        cfg=cfg,
                        store=store,
                        feishu=feishu_app,
                    )
                    """
                    如果是 app 模式，根据 payload 处理告警
                    """
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"ok")
            except Exception as e:
                print(f"alert-bridge error: {e!r}", flush=True)
                traceback.print_exc()
                self.send_response(500)
                self.end_headers()
                msg = str(e)
                self.wfile.write(("error: " + msg[:500]).encode("utf-8", errors="replace"))

        def log_message(self, _format: str, *_args: Any) -> None:  # noqa: D401
            return

    httpd = HTTPServer((cfg.listen_host, cfg.listen_port), Handler)   # 绑定端口、监听的主机以及请求处理器
    httpd.serve_forever()  # 启动 HTTP 服务，等待请求



def main(argv: list[str] | None = None) -> int:
    """入口：读取参数/环境变量并启动服务。"""
    p = build_parser()  # 构建命令行参数解析器
    args = p.parse_args(argv)  # 解析命令行参数
    mode = (args.mode or os.getenv("FEISHU_MODE") or "webhook").strip().lower()
    webhook = args.feishu_webhook or os.getenv("FEISHU_WEBHOOK")
    report_dir = (args.report_dir or os.getenv("INSPECTOR_REPORT_DIR") or "/data/out").strip() or "/data/out"
    report_max_results = int(os.getenv("INSPECTOR_REPORT_MAX_RESULTS") or args.report_max_results or 5)
    report_max_items = int(os.getenv("INSPECTOR_REPORT_MAX_ITEMS") or args.report_max_items or 3)
    chat_tpl = (
        args.chat_name_template
        or os.getenv("FEISHU_CHAT_NAME_TEMPLATE")
        or "K8s告警-{cluster}-{alertname}"
    )
    resolved_suffix = args.resolved_suffix or os.getenv("FEISHU_CHAT_RESOLVED_SUFFIX") or "[已解决]"
    open_ids_raw = args.alert_user_open_ids or os.getenv("FEISHU_ALERT_USER_OPEN_IDS") or ""
    open_ids = [s.strip() for s in open_ids_raw.split(",") if s.strip()]
    state_path = args.state_path or os.getenv("FEISHU_CHAT_STATE_PATH") or "/data/state/feishu_chats.json"

    if mode == "webhook" and not webhook:
        raise SystemExit("webhook 模式缺少 FEISHU_WEBHOOK（或 --feishu-webhook）")
    if mode == "app" and not open_ids:
        raise SystemExit("app 模式缺少 FEISHU_ALERT_USER_OPEN_IDS（逗号分隔 open_id）")
    cfg = BridgeConfig(
        mode=mode,
        feishu_webhook=webhook,
        feishu_chat_name_template=str(chat_tpl),
        feishu_chat_resolved_suffix=str(resolved_suffix),
        feishu_alert_user_open_ids=open_ids,
        state_path=str(state_path),
        report_dir=str(report_dir),
        report_max_results=report_max_results,
        report_max_items=report_max_items,
        listen_port=int(args.listen_port),
        timeout_s=float(args.timeout),
    )
    run_server(cfg)
    return 0



def _handle_alert_payload_app_mode(
    payload: dict[str, Any],
    cfg: BridgeConfig,
    store: JsonStateStore,
    feishu: FeishuAppClient,
) -> None:
    """
    app 模式核心逻辑：
    - firing：创建/复用群并发卡片
    - resolved：修改群名为 xxx[已解决] 并发恢复卡片
    """

    alerts = payload.get("alerts")
    if not isinstance(alerts, list):
        alerts = []
    firing = [a for a in alerts if isinstance(a, dict) and a.get("status") == "firing"]
    resolved = [a for a in alerts if isinstance(a, dict) and a.get("status") == "resolved"]

    key = _derive_group_key(payload, firing or resolved)  # 拿到group_key
    labels = _pick_labels(firing or resolved)  # 提取告警标签（cluster、profile、...）
    base_name = _render_chat_name(cfg.feishu_chat_name_template, labels)
    record = store.get_chat(key)  # 传入group_key，获取到对应的群聊的数据类.

    if firing:
        if record is None:  # 如果说没找到对应的chat_id，就是说对应的群聊咱还没有创建过，那就创建新群。
            chat_id = feishu.create_chat(
                name=base_name,
                description="k8s-inspector alert group",
                user_open_ids=cfg.feishu_alert_user_open_ids,
            )
            record = ChatRecord(chat_id=chat_id, base_name=base_name, resolved=False)  # 构造群聊数据类，记录本次创建的群聊的基本信息
            store.upsert_chat(key, record)   # 将group_key到chat_id的映射关系记录下来
        else:
            if record.base_name != base_name:   # 如果说原本存储的数据类所记录的群聊的名称和本次渲染获得的群聊名称不同，那就更新群聊名称（更新成本次渲染获得的结果）
                record = ChatRecord(chat_id=record.chat_id, base_name=base_name, resolved=record.resolved)
                store.upsert_chat(key, record)
            if record.resolved:
                feishu.rename_chat(record.chat_id, record.base_name)
                store.mark_resolved(key, False)

        lines = ["状态：FIRING", f"cluster: {labels.get('cluster','')}", f"profile: {labels.get('profile','')}"]
        lines.extend(_format_alert_lines(firing, limit=12))
        detail = _build_report_detail_lines(
            report_dir=cfg.report_dir,
            profile=labels.get("profile", ""),
            check_hint=labels.get("check", ""),
            max_results=cfg.report_max_results,
            max_items=cfg.report_max_items,
        )
        if detail:
            lines.append("----")
            lines.extend(detail)
        card = build_alert_card(title="🚨 K8s 巡检告警", lines=lines, template="red")
        feishu.send_card_message(record.chat_id, card)
        return

    """
    这里我是有点疑问的，看这个判定条件：
    - 如果说有告警状态为 resolved，且对应的群聊存在，且对应的群聊未被标记为 resolved，那就修改群聊名称为 xxx[已解决] 并并发恢复卡片。
    我实在费解，因为：altermanager推过来的payload中存在resolved的告警，之前也创建过群聊同时这个群聊还没被标记为resolved，这三个条件感觉并不能就说明所有的alert都被resolved了，你说对吗？
    如果说再加上一个firing为空的并列条件，感觉才能说的过来。

    但是后面我又想了一想，我们看if firing的那个条件分支里，在firing非空的情况下进入到这个分支中，经过一系列处理后将firing的告警推送到飞书，最后return。
    可以说一旦进入了if firing这个分支，这个函数最终就会在这个分支中结束，不会继续后续的程序，也就是说不会实现改名这个操作。
    但是如果说进入了resolved这个分支，那就已经说明了firing是个空列表，所有的告警都已经resolved了，那就是直接改群名了。
    """
    if resolved and record is not None and not record.resolved:   
        new_name = f"{record.base_name}{cfg.feishu_chat_resolved_suffix}"
        feishu.rename_chat(record.chat_id, new_name)
        store.mark_resolved(key, True)
        lines = ["状态：RESOLVED", f"cluster: {labels.get('cluster','')}", f"profile: {labels.get('profile','')}"]
        lines.extend(_format_alert_lines(resolved, limit=12))
        card = build_alert_card(title="✅ K8s 告警已恢复", lines=lines, template="green")
        feishu.send_card_message(record.chat_id, card)

# 通过group_key提取对应的chat_id
def _derive_group_key(payload: dict[str, Any], alerts: list[dict[str, Any]]) -> str:
    """尽量稳定地为“同一批告警”生成唯一 key，用于群聊复用与状态跟踪。

    给同一组告警生成一个用不重复的稳定唯一标识（group key），用来保证：同一种告警只发一条消息，恢复时能找到原来的消息更新。
    也就是：告警去重+消息复用+状态跟踪=全靠这个key。
    - 如果说 groupKey 存在，那就直接使用 groupKey 作为 key。
    - 如果说 groupLabels 存在，那就使用 groupLabels 作为 key。
    - 如果说 groupKey 与 groupLabels 都不存在，那就使用 alertname、cluster、profile、check 作为 key。
    """
    gk = payload.get("groupKey")
    if isinstance(gk, str) and gk.strip():
        return gk.strip()
    gl = payload.get("groupLabels")
    if isinstance(gl, dict) and gl:
        parts = [f"{k}={gl.get(k)}" for k in sorted(gl.keys())]
        return "groupLabels:" + "|".join(parts)  # 将它拼成字符串：groupLabels:alertname=HighCpu|cluster=prod|severity=critical
    # 兜底方案 --- 从告警标签里提取关键维度作为key
    labels = _pick_labels(alerts)
    parts = [f"{k}={labels.get(k,'')}" for k in ["cluster", "profile", "alertname", "check"]]
    return "labels:" + "|".join(parts)


def _pick_labels(alerts: list[dict[str, Any]]) -> dict[str, str]:
    """从告警 labels 中提取常用字段（用于群名模板/消息展示）。"""
    if not alerts:
        return {}
    a0 = alerts[0]  
    labels = a0.get("labels") if isinstance(a0.get("labels"), dict) else {}
    out: dict[str, str] = {}
    for k in ["cluster", "profile", "alertname", "check", "severity"]:
        v = labels.get(k)
        if isinstance(v, str):
            out[k] = v
    # 最终能获取的是一个标签字典
    return out


def _render_chat_name(template: str, labels: dict[str, str]) -> str:
    """将群名模板渲染为最终群名，并做兜底与长度限制。
    chat_tpl = (
        args.chat_name_template
        or os.getenv("FEISHU_CHAT_NAME_TEMPLATE")
        or "K8s告警-{cluster}-{alertname}"
    )
    """
    safe = dict(labels)
    safe.setdefault("cluster", "unknown")  # 保险操作（setdefault），如果说labels中没有cluster，那么就把它填充为unknown
    safe.setdefault("profile", "unknown")
    safe.setdefault("alertname", "alert")
    safe.setdefault("check", "")
    # 防止标签缺失导致格式化报错
    try:
        name = template.format(**safe)  # 格式化填充群名模板中的占位符，比如{cluster}、{alertname}等
        return name[:60] if isinstance(name, str) else "K8s告警"  # 长度限制（飞书群名/标题不能太长）
    except Exception:
        return "K8s告警"


def _format_alert_lines(alerts: list[dict[str, Any]], limit: int) -> list[str]:
    """把多条告警格式化为卡片中的多行文本，避免刷屏（限制最大条数）。"""
    out: list[str] = []
    for a in alerts[:limit]:
        # 遍历告警信息列表中的每条告警，提取出告警标签、注释、名称、检查项、严重程度、摘要
        labels = a.get("labels") if isinstance(a.get("labels"), dict) else {}
        ann = a.get("annotations") if isinstance(a.get("annotations"), dict) else {}
        name = labels.get("alertname") or "Alert"
        check = labels.get("check")
        sev = labels.get("severity")
        summary = ann.get("summary") or ann.get("message") or ""
        parts = [str(name)]
        if sev:
            parts.append(f"sev={sev}")
        if check:
            parts.append(f"check={check}")
        header = " ".join(parts)
        if summary:
            out.append(f"- {header}: {summary}")
        else:
            out.append(f"- {header}")
        """
        - K8sInspectorCheckFail sev=warning check=pod_status: 巡检异常
        """
    if len(alerts) > limit:
        out.append(f"- ... and {len(alerts) - limit} more")
        """
        [
            "- K8sInspectorCheckFail sev=warning check=pod_status: 巡检异常",
            "- K8sNodeHighCpu sev=critical check=nodes: CPU使用率过高",
            "- ... and 3 more"
        ]
        输出的是一个列表哈。
        """
    return out


def _build_report_detail_lines(
    report_dir: str,
    profile: str,
    check_hint: str,
    max_results: int,
    max_items: int,
) -> list[str]:
    report, path = _try_load_latest_report_json(report_dir=report_dir, profile=profile)
    if report is None:
        return []

    lines: list[str] = []
    generated_at = report.get("generated_at")
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    results = report.get("results") if isinstance(report.get("results"), list) else []

    if isinstance(path, str) and path:
        lines.append(f"report: {path}")
    if isinstance(generated_at, str) and generated_at:
        lines.append(f"generated_at: {generated_at}")
    if isinstance(summary, dict) and summary:
        parts: list[str] = []
        for k in ["total", "pass", "warn", "fail", "skip"]:
            v = summary.get(k)
            if isinstance(v, int):
                parts.append(f"{k}={v}")
        if parts:
            lines.append("summary: " + " ".join(parts))

    issue_results: list[dict[str, Any]] = []
    for r in results:
        if not isinstance(r, dict):
            continue
        st = r.get("status")
        if st in {"FAIL", "WARN"}:
            issue_results.append(r)

    if check_hint:
        issue_results.sort(key=lambda r: 0 if r.get("name") == check_hint else 1)

    if not issue_results:
        lines.append("checks: (no FAIL/WARN in report)")
        return lines

    lines.append("checks:")
    shown = 0
    for r in issue_results:
        if shown >= max_results:
            break
        name = r.get("name") if isinstance(r.get("name"), str) else "unknown"
        st = r.get("status") if isinstance(r.get("status"), str) else "UNKNOWN"
        s = r.get("summary") if isinstance(r.get("summary"), str) else ""
        header = f"- [{st}] {name}"
        if s:
            header += f": {s}"
        lines.append(_trim_line(header, 180))

        data = r.get("data") if isinstance(r.get("data"), dict) else {}
        data_line = _summarize_check_data(name=name, data=data, max_items=max_items)
        if data_line:
            lines.append(_trim_line("  " + data_line, 220))
        shown += 1

    if len(issue_results) > shown:
        lines.append(f"- ... and {len(issue_results) - shown} more")
    return lines


def _try_load_latest_report_json(report_dir: str, profile: str) -> tuple[dict[str, Any] | None, str | None]:
    report_dir = (report_dir or "").strip()
    profile = (profile or "").strip()
    if not report_dir:
        return None, None

    candidates: list[str] = []
    if profile:
        candidates.append(os.path.join(report_dir, "latest", profile, "report.json"))
        if profile in {"hourly", "daily"}:
            archive_root = os.path.join(report_dir, "archive", profile)
            latest = _pick_latest_subdir(archive_root)
            if latest:
                candidates.append(os.path.join(archive_root, latest, "report.json"))
    else:
        for p in ["5m", "10m", "hourly", "daily", "full"]:
            candidates.append(os.path.join(report_dir, "latest", p, "report.json"))

    for fp in candidates:
        try:
            if os.path.isfile(fp):
                with open(fp, "r", encoding="utf-8") as f:
                    obj = json.load(f)
                if isinstance(obj, dict):
                    return obj, fp
        except Exception:
            continue
    return None, None


def _pick_latest_subdir(root: str) -> str | None:
    try:
        if not os.path.isdir(root):
            return None
        subs = [n for n in os.listdir(root) if os.path.isdir(os.path.join(root, n))]
        if not subs:
            return None
        subs.sort()
        return subs[-1]
    except Exception:
        return None


def _summarize_check_data(name: str, data: dict[str, Any], max_items: int) -> str:
    if not isinstance(data, dict) or not data:
        return ""

    exclude = {"utilization"}
    prefer_keys: list[str] = []
    if name == "nodes":
        prefer_keys = ["not_ready", "over_cpu", "over_memory", "noschedule_taints"]
    elif name == "pods":
        prefer_keys = ["bad", "restart_heavy", "restart_threshold"]
    elif name == "pvcs":
        prefer_keys = ["pending"]
    elif name == "events":
        prefer_keys = ["count", "sample"]
    elif name == "control-plane":
        prefer_keys = ["apiserver_healthz", "error"]
    else:
        prefer_keys = []

    keys: list[str] = []
    for k in prefer_keys:
        if k in data:
            keys.append(k)
    for k in data.keys():
        if k in exclude:
            continue
        if k not in keys:
            keys.append(k)
        if len(keys) >= 3:
            break

    parts: list[str] = []
    for k in keys:
        v = data.get(k)
        if isinstance(v, list):
            parts.append(f"{k}={len(v)}{_preview_list(v, max_items=max_items)}")
        elif isinstance(v, dict):
            parts.append(f"{k}={_preview_dict(v)}")
        elif isinstance(v, (str, int, float, bool)) or v is None:
            parts.append(f"{k}={v}")
        else:
            parts.append(f"{k}={type(v).__name__}")
    return "; ".join(parts)


def _preview_list(v: list[Any], max_items: int) -> str:
    if max_items <= 0 or not v:
        return ""
    items = []
    for it in v[:max_items]:
        items.append(_compact_item(it))
    s = ", ".join(items)
    return f" ({s})" if s else ""


def _compact_item(it: Any) -> str:
    if isinstance(it, str):
        return _trim_line(it, 80)
    if isinstance(it, (int, float, bool)) or it is None:
        return str(it)
    if isinstance(it, dict):
        for keys in [
            ("namespace", "name", "issue"),
            ("namespace", "name"),
            ("name", "issue"),
            ("node",),
        ]:
            vals = []
            ok = True
            for k in keys:
                v = it.get(k)
                if not isinstance(v, str) or not v:
                    ok = False
                    break
                vals.append(v)
            if ok and vals:
                return "/".join(vals)
        try:
            s = json.dumps(it, ensure_ascii=False, separators=(",", ":"))
            return _trim_line(s, 120)
        except Exception:
            return "{...}"
    try:
        s = str(it)
        return _trim_line(s, 120)
    except Exception:
        return "<item>"


def _preview_dict(d: dict[str, Any]) -> str:
    try:
        keys = list(d.keys())
        if len(keys) <= 6:
            s = json.dumps(d, ensure_ascii=False, separators=(",", ":"))
            return _trim_line(s, 140)
        return "{...}"
    except Exception:
        return "{...}"


def _trim_line(s: str, max_len: int) -> str:
    s = s.replace("\n", " ").replace("\r", " ")
    if len(s) <= max_len:
        return s
    return s[: max(0, max_len - 3)] + "..."


if __name__ == "__main__":
    raise SystemExit(main())
