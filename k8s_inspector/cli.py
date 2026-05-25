"""
命令行入口模块（CLI）。

你运行项目时，最先进入这里：

  python -m k8s_inspector.cli run ...
  初始指定程序的入口，解析命令行参数（在 build_parser 中定义），调用配置加载、K8s 连接、巡检执行，写出报告文件，可选推送到飞书群机器人，最后用退出码表达本次巡检结论。

职责：
1) 解析命令行参数（如何连接集群、输出到哪里、输出哪些格式、按什么频率跑哪些巡检项）
    本地连接的话，需要指定 kubeconfig、context、in-cluster 参数。
    输出的话，需要指定 --output-dir 、--formats 参数。
    执行频率的话，需要指定 --profile 参数。
2) 调用配置加载、K8s 连接、巡检执行
3) 写出报告文件（json/csv/html）
4) 可选：将 FAIL/WARN 摘要推送到飞书群机器人
5) 最后用退出码表达本次巡检结论（0/1/2）
"""

from __future__ import annotations

import argparse
"""
argparse这个模块是专门用来解析命令行参数的。
让你的Python脚本可以像系统命令一样，接收外部传入的参数。
我们用它来解析 run 子命令的参数。
简单说，就是根据用户输入的命令行参数，构建一个 argparse.ArgumentParser 对象，然后调用 parse_args() 方法解析参数。
解析后的参数会被赋值给 args 变量，后续代码可以根据这个变量来访问用户输入的参数。
（比如 args.kubeconfig、args.context 等）
"""
import os
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .checks import run_all
from .config import load_config
from .kube import load_kube
from .metrics import PushGatewayConfig, push_report_to_gateway
from .models import Report
from .notify import send_feishu_text
from .report import format_failed_summary, write_csv, write_html, write_json


def _now_iso() -> str:
    """返回当前时间的 ISO8601 字符串（本地时区，精确到秒）。"""
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    # '2025-12-29T10:00:00+08:00'


def _now_dt() -> datetime:
    return datetime.now(timezone.utc).astimezone()


def _should_persist(report: Report, persist_level: str) -> bool:
    p = (persist_level or "all").strip().lower()
    if p in {"none", "never", "off"}:
        return False
    if p in {"issue", "warn", "fail"}:
        return report.has_warn or report.has_fail
    return True
    # 持久化策略有三个：all、issue、none
    # all：每次都写报告
    # issue：仅当 WARN/FAIL 时写报告
    # none：不写报告
    # 来看这个函数的结果可能哈：如果说传入的是none或是其他的，那么就是返回False（0）；不然的话无论是all还是issue，都是返回True（1）
    # 虽然感觉有点疑惑，毕竟all和issue最终的持久化内容是不完全相同的，但是又想了想，这个函数好像也只是决定是否要持久化的，不管怎么持久化。

"""
下面这个函数是决定在写结果的时候，改往哪个地方写呢？有三种情况：
    - none：直接写到 output-dir
    - hourly：按小时归档，写到 output-dir 下的子目录
    - daily：按天归档，写到 output-dir 下的子目录
咱们同时传入基础目录 base_out_dir，根据 archive 参数，返回最终的输出目录。
- 如果是none的话，那传入哪个目录就返回哪个目录
- 如果是hourly的话，那返回 output-dir 下的子目录，目录名是当前时间的 HH 格式
- 如果是daily的话，那返回 output-dir 下的子目录，目录名是当前时间的 YYYYMMDD 格式
"""
def _resolve_output_dir(base_out_dir: str, archive: str, now: datetime) -> str:
    a = (archive or "none").strip().lower()
    if a in {"none", "flat"}:
        return base_out_dir
    p = Path(base_out_dir)
    if a == "hourly":
        return str(p / now.strftime("%Y%m%d%H"))
    if a == "daily":
        return str(p / now.strftime("%Y%m%d"))
    return base_out_dir


def build_parser() -> argparse.ArgumentParser:
    """
    构建 CLI 参数解析器。

    本项目目前只有一个子命令：run（执行巡检）。
    """
    # 创建主解析器对象
    p = argparse.ArgumentParser(prog="k8s-inspector")  # 脚本名（默认：sys.argv[0]）
    # 创建子命令组解析器对象
    sub = p.add_subparsers(dest="command", required=True)
    # dest这个参数是用于指定子命令的参数名，解析后会被赋值给 args.command 变量。
    # 例如：python -m k8s_inspector.cli run --kubeconfig ~/.kube/config --formats json,html --output-dir ./out --profile full
    # 会将 args.command 赋值为 "run"，
    # dest="command" 是为了后续根据子命令名来判断要执行哪个函数
    # 例如：args.command == "run" 就会执行 cmd_run 函数
    # required=True 是为了强制要求用户输入子命令名
    """
    add_subparsers是专门用来实现【命令行子命令】（像 git commit、docker run、kubectl get这种格式的）
    python -m k8s_inspector.cli run --kubeconfig ~/.kube/config --formats json,html --output-dir ./out --profile full
    就是要实现上面这行启动命令的格式:python -m xxx run --args ...

    子命令组解析器对象，用于添加 run命令的参数。
    run命令的参数包括：
    - --kubeconfig：K8s 集群配置文件路径（可选）
    - --context：K8s 上下文名称（可选）
    - --in-cluster：是否在集群内部运行（默认选中）
    - --config：JSON/YAML 配置文件路径（可选）
    - --profile：执行频率预置（默认 full）
    - --output-dir：输出目录（默认 ./out）
    - --formats：输出格式，逗号分隔（json,csv,html）
    - --fail-on：退出码策略（fail=有 FAIL 才非 0；warn=有 WARN/FAIL 都非 0）
    - --feishu-webhook：飞书群机器人 webhook（仅推送 FAIL/WARN，可选；也可用环境变量 FEISHU_WEBHOOK）
    - --cluster：集群标识（可选，优先级最高）。也可用环境变量 INSPECTOR_CLUSTER。用于指标/告警/群名区分不同集群。
    - --persist-level：落盘策略（all=每次都写报告；issue=仅当 WARN/FAIL 时写报告；none=不写报告）
    - --archive：归档模式（none=直接写到 output-dir；hourly/daily=在 output-dir 下按时间创建子目录归档）
    """

    run = sub.add_parser("run", help="执行巡检")  # 这里就是添加了run这个子命令
    # 然后为这个run命令添加参数
    run.add_argument("--kubeconfig", default=None)
    run.add_argument("--context", default=None)
    run.add_argument("--in-cluster", action="store_true", default=False)
    run.add_argument("--config", default=None, help="JSON/YAML 配置文件路径（可选）")
    run.add_argument(
        "--cluster",
        default=None,
        help="集群标识（可选，优先级最高）。也可用环境变量 INSPECTOR_CLUSTER。用于指标/告警/群名区分不同集群。",
    )
    
    run.add_argument(
        "--profile",
        default="full",
        choices=["5m", "10m", "hourly", "daily", "full"],
        help="执行频率预置：5m=数据平面关键项；10m=控制平面；hourly=基础设施关键项；daily/full=全量",
    )
    # 为--output-dir参数添加注释说明，说明默认输出目录路径为./out
    run.add_argument("--output-dir", default="./out")
    run.add_argument(
        "--formats",
        default="json",
        help="输出格式，逗号分隔：json,csv,html",
    )
    run.add_argument(
        "--persist-level",
        default="all",
        choices=["all", "issue", "none"],
        help="落盘策略：all=每次都写报告；issue=仅当 WARN/FAIL 时写报告；none=不写报告",
    )
    run.add_argument(
        "--archive",
        default="none",
        choices=["none", "hourly", "daily"],
        help="归档模式：none=直接写到 output-dir；hourly/daily=在 output-dir 下按时间创建子目录归档",
    )
    run.add_argument(
        "--fail-on",
        default="fail",
        choices=["fail", "warn"],
        help="退出码策略：fail=有 FAIL 才非 0；warn=有 WARN/FAIL 都非 0",
    )
    run.add_argument(
        "--feishu-webhook",
        default=None,
        help="飞书群机器人 webhook（仅推送 FAIL/WARN，可选；也可用环境变量 FEISHU_WEBHOOK）",
    )
    run.add_argument(
        "--pushgateway-url",
        default=None,
        help="Pushgateway 地址（可选；也可用环境变量 PUSHGATEWAY_URL），用于将巡检结果推送为 Prometheus 指标",
    )
    run.add_argument(
        "--pushgateway-job",
        default=None,
        help="Pushgateway job 名（可选；也可用环境变量 PUSHGATEWAY_JOB，默认 k8s-inspector）",
    )
    run.add_argument(
        "--pushgateway-timeout",
        default=5.0,
        type=float,
        help="Pushgateway 推送超时（秒，默认 5）",
    )
    run.add_argument(
        "--exit-mode",
        default=None,
        choices=["strict", "always0"],
        help="退出码模式：strict=按 FAIL/WARN 返回 0/1/2；always0=始终返回 0（推荐配合 Prometheus/Alertmanager 告警）",
    )

    cleanup = sub.add_parser("cleanup", help="清理归档目录（按保留天数删除旧文件夹）")
    cleanup.add_argument(
        "--root",
        default="./out/archive",
        help="归档根目录（默认 ./out/archive）。目录结构示例：root/hourly/YYYYmmddHH、root/daily/YYYYmmdd",
    )
    cleanup.add_argument("--hourly-days", default=7, type=int, help="hourly 归档保留天数（默认 7）")
    cleanup.add_argument("--daily-days", default=30, type=int, help="daily 归档保留天数（默认 30）")

    return p


def cmd_run(args: argparse.Namespace) -> int:
    """
    执行巡检主流程。

    主要步骤：
    - load_config：加载阈值/控制平面 namespace 等配置
    - load_kube：根据 kubeconfig 或 in-cluster 方式连接集群
    - run_all：按 profile（频率档位）选择巡检项并执行，得到 CheckResult 列表
    - 生成 Report，并按 formats 输出到 output-dir
    - 可选：飞书 webhook 推送 FAIL/WARN 摘要
    - 返回退出码：
      - 2：存在 FAIL
      - 1：fail-on=warn 且存在 WARN
      - 0：其他情况
    """
    """执行巡检主流程。"""
    cfg = load_config(args.config)
    # config.py 中定义了阈值/控制平面 namespace 等配置，负责把“阈值/参数”变成结构化配置。
    # 因为有事先定义好的模板，所以大多数情况下不需要自定义配置，也就是说不用再传入 config 参数。
    # 如果需要自定义配置，可以通过 config 参数传入 JSON/YAML 文件路径。
    # 例如：python -m k8s_inspector.cli run --config custom.yaml
    # 其中 custom.yaml 是自定义的阈值/参数配置文件。

    kube = load_kube(kubeconfig=args.kubeconfig, context=args.context, in_cluster=args.in_cluster)
    # kube.py 中定义了 K8s 集群连接逻辑，负责根据 kubeconfig 或 in-cluster 方式连接集群。
    # 例如：python -m k8s_inspector.cli run --kubeconfig ~/.kube/config
    # 其中 ~/.kube/config 是 K8s 集群配置文件路径。
    # 例如：python -m k8s_inspector.cli run --in-cluster
    # 其中 in-cluster 是是否在集群内部运行参数。如果是在集群内部运行，需要传入 --context 参数指定上下文名称。
    # 例如：python -m k8s_inspector.cli run --context default
    # 其中 default 是 K8s 上下文名称，默认值。

    results = run_all(
        core=kube.core,
        storage=kube.storage,
        custom=kube.custom,
        api_client=kube.api_client,
        cfg=cfg,
        profile=args.profile,
    )
    # run_all（checks.py） 中定义了按 profile（频率档位）选择巡检项并执行的逻辑。
    # 例如：python -m k8s_inspector.cli run --profile 5m
    # 其中 5m 是执行频率预置，默认 full。
    """
    可以看到其参数都是从kube中提取的，包括core、storage、custom、api_client、cfg、profile。
    其中core、storage、custom、api_client是K8s API客户端对象，用于与K8s API交互。
    cfg是阈值/控制平面 namespace 等配置，用于定义巡检项的阈值。
    profile是执行频率预置，用于选择巡检项。
    """

    now = _now_dt()
    cluster_name = (
        str(args.cluster).strip()
        if args.cluster and str(args.cluster).strip()
        else (os.getenv("INSPECTOR_CLUSTER") or "").strip()
    )
    report_cluster = cluster_name or kube.display_name  # display_name 是 K8s 上下文名称，默认值
    report = Report(
        generated_at=now.isoformat(timespec="seconds"),
        cluster=report_cluster,
        results=results,
    )

    formats = {f.strip().lower() for f in str(args.formats).split(",") if f.strip()}
    if _should_persist(report, str(args.persist_level)):
        out_dir = _resolve_output_dir(str(args.output_dir), str(args.archive), now)
        if "json" in formats:
            write_json(report, out_dir)
        if "csv" in formats:
            write_csv(report, out_dir)
        if "html" in formats:
            write_html(report, out_dir)

    pushgateway_url = args.pushgateway_url or os.getenv("PUSHGATEWAY_URL")
    if pushgateway_url:
        job = args.pushgateway_job or os.getenv("PUSHGATEWAY_JOB") or "k8s-inspector"
        try:
            push_report_to_gateway(
                report=report,
                profile=args.profile,
                cfg=PushGatewayConfig(
                    url=pushgateway_url, job=job, timeout_s=float(args.pushgateway_timeout)
                ),
            )
        except Exception as e:
            print(f"pushgateway 推送失败：{e}")

    webhook = args.feishu_webhook or os.getenv("FEISHU_WEBHOOK")
    if webhook and (report.has_fail or report.has_warn):
        text = format_failed_summary(report)
        send_feishu_text(webhook, text=text)

    exit_mode = (
        (args.exit_mode or os.getenv("INSPECTOR_EXIT_MODE") or "strict").strip().lower()
    )
    if exit_mode == "always0":
        return 0
        # 如果退出策略是always0的话，那么程序到此为止，以0退出。
    if report.has_fail:
        return 2
    if args.fail_on == "warn" and report.has_warn:
        return 1
    return 0


def _cleanup_subdirs(root: Path, keep_days: int, fmt: str) -> int:
    if keep_days <= 0:
        return 0
    if not root.exists():
        return 0
    now = _now_dt().replace(minute=0, second=0, microsecond=0)  # 取出当前时间，将时分秒清零（只按天比较）
    cutoff = now - timedelta(days=keep_days)  # 算出阈值，超过 N 天前的目录都要删。
    removed = 0
    # 遍历子目录，只遍历子目录，跳过文件
    for p in root.iterdir():
        if not p.is_dir():
            continue
        name = p.name
        """
        只处理纯数字目录名
        - 防止误删非日期格式的目录
        - 比如 20251229会保留，backup会跳过
        """
        if not name.isdigit():
            continue
        try:
            dt = datetime.strptime(name, fmt)  # 按日期格式解析目录名，例如 fmt="%Y%m%d"---解析20251229，解析失败则跳过该目录，不报错。
        except Exception:
            continue
        dt = dt.replace(tzinfo=_now_dt().tzinfo)  # 从目录名解析出的时间不带时区，必须打上本地时区才能正确比较。
        # 删除过期目录，目录日期<阈值--删除
        if dt < cutoff:
            shutil.rmtree(p, ignore_errors=True)  # ignore_errors=True：删不掉也不崩溃（权限/占用等）
            removed += 1
    return removed


def cmd_cleanup(args: argparse.Namespace) -> int:
    root = Path(str(args.root))  # 获取归档根目录，从命令行参数拿到--root（默认./out/archive），转成Path对象方便拼接路径。
    # 拼接目录
    hourly = root / "hourly"  # 每小时目录档根目录
    daily = root / "daily"  # 每天目录档根目录
    """
    archive/
  ├── hourly/
  │     ├── 2025122910/
  │     ├── 2025122911/
  │     └── ...
  └── daily/
        ├── 20251229/
        ├── 20251228/
        └── ...
    """
    # 调用清理工具函数
    removed_hourly = _cleanup_subdirs(hourly, int(args.hourly_days), "%Y%m%d%H")
    removed_daily = _cleanup_subdirs(daily, int(args.daily_days), "%Y%m%d")
    print(f"cleanup done: hourly_removed={removed_hourly} daily_removed={removed_daily}")
    return 0


def main(argv: list[str] | None = None) -> int:
    """程序入口函数（便于打包成 console_script，也便于单元测试调用）。"""
    p = build_parser()
    args = p.parse_args(argv)
    """
    parse_args函数是用来解析命令行参数，返回 argparse.Namespace 对象。
    这个argv参数是命令行参数列表，默认值为 None。

    也就是说先是构建解析器对象，调用解析器对象的parse_args方法解析命令行参数，最后返回 argparse.Namespace 对象。
    """
    if args.command == "run":
        return cmd_run(args)
    if args.command == "cleanup":
        return cmd_cleanup(args)
    raise SystemExit(2)
    # 这个 raise SystemExit(2) 是为了在命令行中显示错误信息，退出码为 2 表示有错误发生。


if __name__ == "__main__":
    raise SystemExit(main())
    """
    SystemExit这个异常是用来退出程序的，退出码为 2 表示有错误发生。
    """
