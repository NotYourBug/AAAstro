"""
状态存储模块（用于告警拉群场景）。

在“飞书应用机器人（app 模式）”下，桥接服务会：
- 第一次收到某条告警分组（groupKey/groupLabels）时创建一个群聊；
- 之后同一个分组继续告警时复用同一个群，避免重复拉群；
- 告警 resolved 时，需要找到对应群聊并修改群名为 xxx[已解决]。

因此需要一个“分组 key -> chat_id”的映射存储。
这里用 JSON 文件做最轻量的持久化（足够演示与小规模使用），生产可替换为 Redis/DB。
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class ChatRecord:
    """一个告警分组对应的群聊记录。"""
    chat_id: str    # 飞书群聊 ID
    base_name: str  # 基础群名（未解决时使用）
    resolved: bool  # 是否已解决
# 咱就是说一个告警分组对应一条飞书消息


class JsonStateStore:
    """
    轻量 JSON 状态存储。

    文件结构示例：
    {
      "<group_key>": {"chat_id": "...", "base_name": "...", "resolved": false}
    }

    把告警状态存在本地JSON文件，作用：
      - 告警再次触发时，根据 group_key 找到对应的 chat_id，避免重复拉群；
      - 告警 resolved 时，更新 resolved 状态，记录已解决；
      - 告警恢复时更新卡片为已恢复
      - 避免重复刷屏
      - 保持状态文件的持久化，避免因服务重启或崩溃导致状态丢失
    """
    def __init__(self, path: str):
        self._path = Path(path)

    # 查告警是否已发过
    def get_chat(self, key: str) -> ChatRecord | None:
        """读取某个 group_key 的 chat 记录；不存在返回 None。"""
        data = self._load()
        it = data.get(key)
        if not isinstance(it, dict):
            return None
        chat_id = it.get("chat_id")
        base_name = it.get("base_name")
        resolved = it.get("resolved")
        if not isinstance(chat_id, str) or not isinstance(base_name, str):
            return None
        return ChatRecord(chat_id=chat_id, base_name=base_name, resolved=bool(resolved))

    # 写入/覆盖某个 group_key 的 chat 记录
    def upsert_chat(self, key: str, record: ChatRecord) -> None:
        """写入/覆盖某个 group_key 的 chat 记录。"""
        data = self._load()
        data[key] = {"chat_id": record.chat_id, "base_name": record.base_name, "resolved": record.resolved}
        self._save(data)

    # 更新 resolved 状态    
    def mark_resolved(self, key: str, resolved: bool) -> None:
        """仅更新 resolved 状态。"""
        data = self._load()
        it = data.get(key)
        if not isinstance(it, dict):
            return
        it["resolved"] = bool(resolved)
        data[key] = it
        self._save(data)

    # 读写文件（原子写入，不会坏）
    def _load(self) -> dict[str, Any]:
        """读取 JSON 文件；异常时返回空 dict（避免因状态文件损坏导致服务不可用）。"""
        try:
            if not self._path.exists():
                return {}
            raw = self._path.read_text(encoding="utf-8")
            obj = json.loads(raw) if raw.strip() else {}
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}

    def _save(self, data: dict[str, Any]) -> None:
        """原子写入：先写临时文件，再 replace 覆盖，减少中途写坏的风险。"""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, self._path)
    
    """
    - 先写临时文件，再 replace 覆盖，减少中途写坏的风险
    - 避免重复刷屏
    - 保持状态文件的持久化，避免因服务重启或崩溃导致状态丢失，断电/崩溃不会写坏JSON
    """