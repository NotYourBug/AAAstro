"""
飞书“企业自建应用机器人”能力封装（基于 lark-oapi SDK）。

为什么需要这个模块：
- 你希望机器人能“自动拉群、拉人、改群名”，这属于飞书开放平台的 IM 群聊管理能力；
- 普通“群自定义机器人 webhook”只能向固定群发消息，无法创建群/改群名；
- 因此这里用 lark-oapi（飞书开放平台 SDK）封装常用操作：
  - 创建群聊（create_chat）
  - 发送群消息（send_text_message / send_card_message）
  - 修改群名称（rename_chat）

本模块只负责调用飞书 API，不负责解析 Alertmanager 告警、分组、状态存储。
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any


@dataclass
class FeishuAppConfig:
    """飞书应用配置。"""
    app_id: str
    app_secret: str
    user_id_type: str = "open_id"


def load_feishu_app_config_from_env() -> FeishuAppConfig:
    """从环境变量读取飞书应用配置（FEISHU_APP_ID / FEISHU_APP_SECRET）。"""
    app_id = os.getenv("FEISHU_APP_ID", "").strip()
    app_secret = os.getenv("FEISHU_APP_SECRET", "").strip()
    if not app_id or not app_secret:
        raise RuntimeError("缺少 FEISHU_APP_ID / FEISHU_APP_SECRET")
    user_id_type = os.getenv("FEISHU_USER_ID_TYPE", "open_id").strip() or "open_id"
    return FeishuAppConfig(app_id=app_id, app_secret=app_secret, user_id_type=user_id_type)


class FeishuAppClient:
    """飞书 IM API 的最小封装客户端。"""
    def __init__(self, cfg: FeishuAppConfig):
        self._cfg = cfg
        self._client = _build_lark_client(cfg.app_id, cfg.app_secret)

    def create_chat(self, name: str, description: str, user_open_ids: list[str]) -> str:
        """创建群聊并邀请成员（user_id_list）。返回 chat_id。"""
        im_v1 = _import_im_v1()
        req = (
            im_v1.CreateChatRequest.builder()
            .user_id_type(self._cfg.user_id_type)
            .request_body(
                im_v1.CreateChatRequestBody.builder()
                .name(name)
                .description(description)
                .user_id_list(user_open_ids)
                .build()
            )
            .build()
        )
        resp = self._client.im.v1.chat.create(req)
        if not resp.success():
            raise RuntimeError(f"创建告警群失败: {resp.msg}, code: {resp.code}")
        return resp.data.chat_id

    def send_card_message(self, chat_id: str, card: dict[str, Any]) -> None:
        """向群聊发送“交互卡片（interactive）”消息。"""
        im_v1 = _import_im_v1()
        lark = _import_lark()
        content = lark.JSON.marshal(card)
        req = (
            im_v1.CreateMessageRequest.builder()
            .receive_id_type("chat_id")
            .request_body(
                im_v1.CreateMessageRequestBody.builder()
                .receive_id(chat_id)
                .msg_type("interactive")
                .content(content)
                .build()
            )
            .build()
        )
        resp = self._client.im.v1.message.create(req)
        if not resp.success():
            raise RuntimeError(f"发送告警消息失败: {resp.msg}, code: {resp.code}")

    def send_text_message(self, chat_id: str, text: str) -> None:
        """向群聊发送普通 text 消息。"""
        im_v1 = _import_im_v1()
        payload = {"text": text}
        req = (
            im_v1.CreateMessageRequest.builder()
            .receive_id_type("chat_id")
            .request_body(
                im_v1.CreateMessageRequestBody.builder()
                .receive_id(chat_id)
                .msg_type("text")
                .content(json.dumps(payload, ensure_ascii=False))
                .build()
            )
            .build()
        )
        resp = self._client.im.v1.message.create(req)
        if not resp.success():
            raise RuntimeError(f"发送文本消息失败: {resp.msg}, code: {resp.code}")

    def rename_chat(self, chat_id: str, new_name: str) -> None:
        """修改群名称。会尝试 patch/update 两种接口以兼容不同 SDK 版本。"""
        im_v1 = _import_im_v1()
        patch_cls = getattr(im_v1, "PatchChatRequest", None)
        patch_body_cls = getattr(im_v1, "PatchChatRequestBody", None)
        if patch_cls and patch_body_cls:
            req = (
                patch_cls.builder()
                .chat_id(chat_id)
                .request_body(patch_body_cls.builder().name(new_name).build())
                .build()
            )
            resp = self._client.im.v1.chat.patch(req)
            if not resp.success():
                raise RuntimeError(f"修改群名失败: {resp.msg}, code: {resp.code}")
            return

        update_cls = getattr(im_v1, "UpdateChatRequest", None)
        update_body_cls = getattr(im_v1, "UpdateChatRequestBody", None)
        if update_cls and update_body_cls:
            req = (
                update_cls.builder()
                .chat_id(chat_id)
                .request_body(update_body_cls.builder().name(new_name).build())
                .build()
            )
            resp = self._client.im.v1.chat.update(req)
            if not resp.success():
                raise RuntimeError(f"修改群名失败: {resp.msg}, code: {resp.code}")
            return

        raise RuntimeError("当前 lark-oapi 版本未找到群名修改接口（PatchChatRequest/UpdateChatRequest）")


def _build_lark_client(app_id: str, app_secret: str):
    """构建 lark-oapi client（使用 app_id/app_secret 做 tenant_access_token 认证）。"""
    lark = _import_lark()
    return lark.Client.builder().app_id(app_id).app_secret(app_secret).build()


def _import_lark():
    """按需导入 lark_oapi，避免未安装依赖时报错不清晰。"""
    try:
        import lark_oapi as lark  # type: ignore

        return lark
    except Exception as e:
        raise RuntimeError("需要安装 lark-oapi 才能使用飞书应用机器人能力") from e


def _import_im_v1():
    """导入 lark_oapi.api.im.v1 模块（包含 Chat/Message 的请求/响应 builder）。"""
    try:
        import importlib

        return importlib.import_module("lark_oapi.api.im.v1")
    except Exception as e:
        raise RuntimeError("lark-oapi 缺少 im.v1 模块，无法调用群聊/消息 API") from e


def build_alert_card(title: str, lines: list[str], template: str = "red") -> dict[str, Any]:
    """构建一个最简飞书交互卡片 payload（用于告警展示）。"""
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": template,
            "title": {"tag": "plain_text", "content": title},
        },
        "elements": [
            {"tag": "div", "text": {"tag": "plain_text", "content": "\n".join(lines)[:4000]}},
        ],
    }
