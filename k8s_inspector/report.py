"""
报告输出模块。

职责：
- 将 Report 模型写出为多种格式（JSON/CSV/HTML），供人查看、归档或二次分析
- 生成“失败摘要文本”，用于飞书等通知渠道

注意：
- 本模块不做 K8s API 调用，也不做巡检规则判断
- 输入是 models.Report，输出是文件或字符串
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from .models import Report, Status


def _ensure_dir(p: Path) -> None:
    """确保输出目录存在。"""
    p.mkdir(parents=True, exist_ok=True) # 确保这个文件夹（目录）一定存在，如果不存在就自动创建；如果已经存在，就什么也不做，也不报错。


# 这里传进来的都是已经初始化好的Report类，我们需要根据已经初始化好的Report实例，对其进行操作，写入特定类型的文件中。
def write_json(report: Report, out_dir: str) -> Path:
    """输出 report.json（结构化明细，最适合存档和后续程序处理）。"""
    p = Path(out_dir)
    _ensure_dir(p)
    fp = p / "report.json"
    """
    这是一种Path语法，比如说我们指定的目录是：./output，
    那么fp就会指向：./output/report.json（p / "report.json" 在这里表示组装路径，将p和"report.json"拼接起来，得到./output/report.json）
    如果./output目录不存在，那么就会自动创建。
    """
    fp.write_text(json.dumps(report.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    """
    这里使用json.dumps函数将Report实例转换为JSON字符串，
    并将字符串写入到fp指向的文件中。
    ensure_ascii=False表示不使用ASCII编码，而是使用本地编码（允许文件里出现中文，不会变成\u4e2d\u6587）。
    indent=2表示缩进2个空格，使JSON字符串更易读。
    """
    return fp


def write_csv(report: Report, out_dir: str) -> Path:
    """输出 report.csv（扁平摘要，适合快速筛选/导入表格工具）。"""
    p = Path(out_dir)
    _ensure_dir(p)
    fp = p / "report.csv"
    with fp.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["generated_at", "cluster", "check", "status", "summary", "duration_ms"])
        for r in report.results:
            w.writerow([report.generated_at, report.cluster, r.name, r.status.value, r.summary, r.duration_ms or ""])
    return fp


def write_html(report: Report, out_dir: str) -> Path:
    """输出 report.html（可视化报告，适合人工复核）。"""
    p = Path(out_dir)
    _ensure_dir(p)
    fp = p / "report.html"
    rows: list[str] = []
    for r in report.results:
        cls = r.status.value.lower()
        rows.append(
            "<tr>"
            f"<td>{_esc(r.name)}</td>"
            f"<td class='{cls}'>{_esc(r.status.value)}</td>"
            f"<td>{_esc(r.summary)}</td>"
            f"<td>{'' if r.duration_ms is None else r.duration_ms}</td>"
            "</tr>"
        )

    summary = report.to_dict().get("summary", {})
    html = f"""<!doctype html>
<html lang="zh">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>K8s 巡检报告</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, "Noto Sans", sans-serif; margin: 24px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; vertical-align: top; }}
    th {{ background: #fafafa; text-align: left; }}
    .pass {{ color: #0a7d30; font-weight: 600; }}
    .warn {{ color: #a66a00; font-weight: 600; }}
    .fail {{ color: #b00020; font-weight: 600; }}
    .skip {{ color: #666; font-weight: 600; }}
    .meta {{ margin: 0 0 12px 0; color: #444; }}
    .pill {{ display: inline-block; padding: 2px 8px; border-radius: 999px; background: #f3f4f6; margin-right: 8px; }}
  </style>
</head>
<body>
  <h1>K8s 巡检报告</h1>
  <p class="meta">
    <span class="pill">generated_at: {_esc(report.generated_at)}</span>
    <span class="pill">cluster: {_esc(report.cluster)}</span>
    <span class="pill">total: {summary.get("total", 0)}</span>
    <span class="pill pass">pass: {summary.get("pass", 0)}</span>
    <span class="pill warn">warn: {summary.get("warn", 0)}</span>
    <span class="pill fail">fail: {summary.get("fail", 0)}</span>
    <span class="pill skip">skip: {summary.get("skip", 0)}</span>
  </p>
  <table>
    <thead>
      <tr><th>Check</th><th>Status</th><th>Summary</th><th>Duration(ms)</th></tr>
    </thead>
    <tbody>
      {''.join(rows)}
    </tbody>
  </table>
</body>
</html>
"""
    fp.write_text(html, encoding="utf-8")
    return fp


def _esc(s: Any) -> str:
    """对 HTML 文本做最小转义，避免破坏页面结构。"""
    text = "" if s is None else str(s)
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def format_failed_summary(report: Report) -> str:
    """
    生成适合消息推送的摘要文本（默认最多展示每类前 10 条）。

    用途：
    - 飞书/钉钉/企业微信 机器人推送时，避免把 report.json 全量发到群里刷屏
    """
    fails = [r for r in report.results if r.status == Status.FAIL]
    warns = [r for r in report.results if r.status == Status.WARN]
    lines = [f"集群巡检：{report.cluster}", f"时间：{report.generated_at}"]
    if fails:
        lines.append(f"FAIL：{len(fails)}")
        for r in fails[:10]:
            lines.append(f"- {r.name}: {r.summary}")
    if warns:
        lines.append(f"WARN：{len(warns)}")
        for r in warns[:10]:
            lines.append(f"- {r.name}: {r.summary}")
    return "\n".join(lines)
    """
    这里使用\n将lines列表中的所有元素连接起来，得到一个字符串。
    每个元素之间用换行符隔开。
    """
"""
从results（CheckReport）中获取所有的fail结果
从results（CheckReport）中获取所有的warn结果
构造信息行内容
将fail的内容传进line中
将warn的内容传进line中
"""
