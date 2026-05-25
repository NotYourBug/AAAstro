"""
通知模块（目前实现：飞书群机器人 Webhook）。

设计目标：
- 与巡检逻辑解耦：巡检只关心“要不要通知、通知什么内容”
- 依赖尽量少：这里使用 Python 标准库 urllib，不额外引入 requests
"""

from __future__ import annotations

import json
import urllib.request
from typing import Any

# webhook通过环境变量导入，非首选模式
def send_feishu_text(webhook_url: str, text: str, timeout_s: float = 5.0) -> dict[str, Any]:
    """
    发送飞书机器人文本消息。

    - webhook_url：飞书机器人地址
    - text：文本内容（建议是摘要，而不是完整 JSON）
    - timeout_s：网络超时

    返回：
    - {"status": http_status, "body": response_body_prefix}
    """
    payload = {"msg_type": "text", "content": {"text": text}}
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=data,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        return {"status": resp.status, "body": body[:2000]}
