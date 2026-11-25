"""QQ 插件包：OneBot v11 HTTP 发送与 WebSocket 消息接收。"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import io
import ipaddress
import json
import re
import time
import wave
import warnings
import uuid
from collections import OrderedDict
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from threading import Condition, Event, Lock, Semaphore, Thread, Timer, current_thread
from urllib.parse import urlsplit

from qq_poke import poke_notice, poke_only_addressed

import httpx
from websockets.asyncio.client import connect


# Match the WebUI composer: at most eight image parts from one message.
# The original QQ download can be larger than the model-bound image; it is
# checked separately and normalized to 2048 px / 6 MiB by resolve_media().
MAX_INCOMING_IMAGES = 8


class _NoSpeechDetected(Exception):
    """Local VAD found no speech; this is not a decoder failure."""



def _ids(value: object, field: str) -> set[str]:
    if not isinstance(value, list):
        raise ValueError(f"{field} 必须是 QQ 号列表")
    ids: set[str] = set()
    for item in value:
        if isinstance(item, bool) or not isinstance(item, (str, int)):
            raise ValueError(f"{field} 只收正整数 QQ 号")
        said = str(item)
        if not said.isascii() or not said.isdecimal() or int(said) <= 0:
            raise ValueError(f"{field} 只收正整数 QQ 号")
        ids.add(str(int(said)))
    return ids


def _sharing_enabled(settings, *, managed_owner):
    enabled = settings.get("sharingEnabled", managed_owner or bool(settings.get("sharingOwner")))
    if type(enabled) is not bool:
        raise ValueError("sharingEnabled 必须是布尔值")
    return enabled


def _config(path: Path, *, managed_owner=False, model_root: Path | None = None, snapshot: dict | None = None
            ) -> tuple[str, str, str, str, set[str], set[str], str, bool, str, str,
                       int, str, str, bool, int, str]:
    if snapshot is None and not path.is_file():
        raise ValueError(f"请先将 config.example.json 复制为 {path.name} 并填写允许发送的 QQ 号")
    got = snapshot if snapshot is not None else json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(got, dict):
        raise ValueError("QQ 配置必须是 JSON 对象")
    url = got.get("httpUrl")
    if not isinstance(url, str):
        raise ValueError("httpUrl 必须是 HTTP 地址")
    parsed = urlsplit(url)
    if (parsed.scheme not in ("http", "https") or not parsed.hostname
            or parsed.username or parsed.password or parsed.query or parsed.fragment):
        raise ValueError("httpUrl 必须是无账号与查询参数的 HTTP(S) 地址")
    token = got.get("token", "")
    if not isinstance(token, str):
        raise ValueError("token 必须是字符串")
    ws_url = got.get("wsUrl", "")
    if not isinstance(ws_url, str):
        raise ValueError("wsUrl 必须是 WebSocket 地址")
    if ws_url:
        parsed_ws = urlsplit(ws_url)
        if (parsed_ws.scheme not in ("ws", "wss") or not parsed_ws.hostname
                or parsed_ws.username or parsed_ws.password
                or parsed_ws.query or parsed_ws.fragment):
            raise ValueError("wsUrl 必须是无账号与查询参数的 WebSocket 地址")
    ws_token = got.get("wsToken", token)
    if not isinstance(ws_token, str):
        raise ValueError("wsToken 必须是字符串")
    privates = _ids(got.get("privates", []), "privates")
    groups = _ids(got.get("groups", []), "groups")
    if not privates and not groups:
        raise ValueError("请在 privates 或 groups 中填写至少一个允许发送的目标")
    proactive = got.get("proactiveTarget", "")
    if proactive not in ("", None):
        proactive = _qq_id(proactive)
        if proactive is None or (not managed_owner and proactive not in privates):
            raise ValueError("proactiveTarget 必须是 privates 名单中的 QQ 号")
    sharing_owner = got.get("sharingOwner", "")
    if _sharing_enabled(got, managed_owner=managed_owner) and not managed_owner:
        if (not isinstance(sharing_owner, str)
                or _qq_id(sharing_owner) != sharing_owner or sharing_owner not in privates
                or proactive != sharing_owner):
            raise ValueError("主动分享只能发送给配置的主人，且主人必须在私聊白名单内")
    transcribe_voice = got.get("transcribeVoice", False)
    if not isinstance(transcribe_voice, bool):
        raise ValueError("transcribeVoice 必须是布尔值")
    tts_url = got.get("ttsUrl", "")
    tts_enabled = got.get("ttsEnabled", bool(tts_url))
    if type(tts_enabled) is not bool:
        raise ValueError("ttsEnabled 必须是布尔值")
    if not isinstance(tts_url, str):
        raise ValueError("ttsUrl 必须是本机 HTTP 地址")
    if tts_url:
        parsed_tts = urlsplit(tts_url)
        if (parsed_tts.scheme != "http" or parsed_tts.hostname not in
                ("127.0.0.1", "localhost") or parsed_tts.username or parsed_tts.password
                or parsed_tts.query or parsed_tts.fragment or parsed_tts.path not in ("", "/")):
            raise ValueError("ttsUrl 必须是无账号与查询参数的本机 HTTP 地址")
    tts_voice = got.get("ttsVoice", "default")
    if tts_voice not in ("default", "telyin"):
        raise ValueError("ttsVoice 必须是可用的音色预设")
    tts_send_mode = got.get("ttsSendMode", "on_demand")
    if tts_send_mode not in ("on_demand", "reply_with_voice"):
        raise ValueError("ttsSendMode 必须是按需或回复附带语音")
    asr_language = got.get("asrLanguage", "auto")
    if asr_language not in ("auto", "zh", "en"):
        raise ValueError("asrLanguage 仅支持 auto、zh、en")
    asr_use_itn = got.get("asrUseItn", True)
    if type(asr_use_itn) is not bool:
        raise ValueError("asrUseItn 必须是布尔值")
    asr_threads = got.get("asrThreads", 3)
    if type(asr_threads) is not int or asr_threads not in (2, 3):
        raise ValueError("asrThreads 仅支持 2 或 3")
    asr_model_dir = got.get("asrModelDir", "")
    if not isinstance(asr_model_dir, str):
        raise ValueError("asrModelDir 必须是本机模型目录")
    from deployment_relocation import effective_asr_directory
    asr_model_dir = effective_asr_directory(asr_model_dir, model_root)
    if not asr_model_dir and model_root is not None:
        preferred = model_root / "voice" / "asr" / "SenseVoiceSmall-onnx"
        if (preferred / "model.int8.onnx").is_file() and (preferred / "tokens.txt").is_file():
            asr_model_dir = str(preferred)
    if asr_model_dir and not all(
        (Path(asr_model_dir) / name).is_file()
        for name in ("model.int8.onnx", "tokens.txt")
    ):
        # An unavailable explicitly selected local model disables only transcription.
        # Do not fall back to sending the audio to another provider.
        transcribe_voice = False
        asr_model_dir = ""
    backfill_minutes = got.get("historyBackfillMinutes", 0)
    if (type(backfill_minutes) is not int or not 0 <= backfill_minutes <= 30):
        raise ValueError("historyBackfillMinutes 必须是 0 到 30 的整数")
    return (url.rstrip("/") + "/", token, ws_url, ws_token,
            privates, groups, proactive or "", transcribe_voice,
            tts_url.rstrip("/") + "/" if tts_url and tts_enabled else "", asr_model_dir,
            backfill_minutes, tts_voice, asr_language, asr_use_itn,
            asr_threads, tts_send_mode)


def _qq_id(value: object) -> str | None:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        return None
    said = str(value)
    if (not said.isascii() or not said.isdecimal() or len(said) > 20
            or int(said) <= 0):
        return None
    return str(int(said))


def _message_id(value: object) -> str | None:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        return None
    said = str(value).strip()
    digits = said[1:] if said.startswith("-") else said
    return said if digits.isascii() and digits.isdecimal() and len(said) <= 20 else None


def _face_id(value: object) -> str | None:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        return None
    said = str(value)
    if not said.isascii() or not said.isdecimal() or len(said) > 6:
        return None
    return str(int(said))


def _emoji_id(value: object) -> str | None:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", value):
        return None
    return value


def _image_format(raw: bytes) -> tuple[str, str]:
    if raw.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png", "image/png"
    if raw.startswith((b"GIF87a", b"GIF89a")):
        return "gif", "image/gif"
    if raw.startswith(b"\xff\xd8\xff"):
        return "jpg", "image/jpeg"
    if raw[:4] == b"RIFF" and raw[8:12] == b"WEBP":
        return "webp", "image/webp"
    raise ValueError("QQ 图片不是支持的 PNG、JPEG、GIF 或 WebP")


def _market_sticker_url(emoji_id: str) -> str:
    return f"https://gxh.vip.qq.com/club/item/parcel/item/{emoji_id[:2]}/{emoji_id}/raw300.gif"


def _incoming_image_kind(data: dict) -> str:
    if data.get("type") == "flash":
        return "image"
    if type(data.get("sub_type")) is int and data["sub_type"] == 1:
        return "sticker"
    # SnowLuma emits market faces as image/sub_type=0 with their native QQ ID.
    emoji_id = data.get("emoji_id")
    if (isinstance(emoji_id, str) and re.fullmatch(r"[0-9a-fA-F]{32}", emoji_id)
            and data.get("url") == _market_sticker_url(emoji_id)):
        return "sticker"
    return "image"


def _cached_media_id(conversation: str, raw: bytes, kind: str | None) -> str:
    prefix = conversation.encode("utf-8") + b"\0"
    if kind is not None:
        if kind not in ("sticker", "image", "file_image"):
            raise ValueError("未知的 QQ 媒体来源")
        prefix = b"qq-media-v2\0" + prefix + kind.encode("ascii") + b"\0"
    return hashlib.sha256(prefix + raw).hexdigest()[:24]


def _write_cached_file(path: Path, raw: bytes) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_bytes(raw)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _trusted_image_url(value: object) -> str:
    if not isinstance(value, str) or len(value) > 4096:
        raise ValueError("QQ 图片没有可用下载地址")
    match = re.search(r"/([0-9a-fA-F]{32})/raw300\.gif$", value)
    if match and value == _market_sticker_url(match[1]):
        return value
    parsed = urlsplit(value)
    host = (parsed.hostname or "").lower()
    if (parsed.scheme != "https" or parsed.username or parsed.password
            or parsed.port not in (None, 443) or parsed.fragment
            or host != "multimedia.nt.qq.com.cn" and not host.endswith(".qpic.cn")):
        raise ValueError("QQ 图片下载地址不在允许域名内")
    return value


def _image_file(data: dict) -> bool:
    name = data.get("name")
    size = data.get("file_size", data.get("size"))
    return (isinstance(name, str) and name.lower().endswith(
        (".png", ".jpg", ".jpeg", ".gif", ".webp"))
        and type(size) is int and 0 < size <= 8 * 1024 * 1024)


def _trusted_file_url(value: object) -> str:
    if not isinstance(value, str) or len(value) > 4096:
        raise ValueError("QQ 文件没有可用下载地址")
    parsed = urlsplit(value)
    host = (parsed.hostname or "").lower()
    try:
        port = parsed.port
    except ValueError:
        raise ValueError("QQ 文件下载地址不符合安全限制") from None
    try:
        public_ip = ipaddress.ip_address(host).is_global
    except ValueError:
        public_ip = False
    public_ip_download = (parsed.scheme in ("http", "https") and public_ip
                          and port in (None, 80, 443)
                          and parsed.path.endswith("/qqdownloadftnv5"))
    qq_ftn_download = (parsed.scheme == "https" and host == "njc-download.ftn.qq.com"
                       and port in (None, 443)
                       and re.fullmatch(r"/ftn_handler/[0-9A-Fa-f]{32,256}/?", parsed.path))
    if (parsed.username or parsed.password or parsed.fragment
            or not (public_ip_download or qq_ftn_download)):
        raise ValueError("QQ 文件下载地址不符合安全限制")
    return value


def _body(message: object, *, voice_text: str | None = None,
          image_ids: list[str] | None = None) -> tuple[str, list[str], list[str], list[str]]:
    if isinstance(message, str):
        return message, ["text"], [], []
    if not isinstance(message, list):
        return "", [], [], []
    parts: list[str] = []
    kinds: list[str] = []
    faces: list[str] = []
    images: list[str] = []
    image_index = 0
    for segment in message:
        if not isinstance(segment, dict):
            continue
        kind = segment.get("type")
        data = segment.get("data")
        if not isinstance(kind, str) or not isinstance(data, dict):
            continue
        kinds.append(kind)
        if kind == "text" and isinstance(data.get("text"), str):
            parts.append(data["text"])
        elif kind == "at":
            who = _qq_id(data.get("qq"))
            parts.append("@全体成员" if data.get("qq") == "all" else f"@{who}" if who else "[@]")
        elif kind == "reply":
            reply = _message_id(data.get("id"))
            parts.append(f"[回复#{reply}]" if reply else "[回复]")
        elif kind == "face":
            face_id = _face_id(data.get("id"))
            parts.append(f"[QQ表情#{face_id}]" if face_id is not None else "[QQ表情]")
            if face_id is not None:
                faces.append(face_id)
        elif kind == "image":
            image_kind = _incoming_image_kind(data)
            images.append(image_kind)
            # OneBot's sub_type=1 means a sticker, not necessarily an animated
            # image. QQ can send a still JPEG with this subtype.
            label = "表情包" if image_kind == "sticker" else "图片"
            media_id = (image_ids[image_index] if image_ids is not None
                        and image_index < len(image_ids) else "")
            parts.append(f"[{label}#{media_id}]" if media_id else f"[{label}]")
            image_index += 1
        elif kind == "file":
            if _image_file(data):
                images.append("file_image")
                media_id = (image_ids[image_index] if image_ids is not None
                            and image_index < len(image_ids) else "")
                parts.append(f"[图片文件#{media_id}]" if media_id else "[图片文件]")
                image_index += 1
            else:
                parts.append("[文件]")
        elif kind == "record":
            if voice_text is None:
                parts.append("[语音]")
            elif voice_text:
                parts.append(f"[语音转写] {voice_text}")
            else:
                parts.append("[语音，转写暂不可用]")
        else:
            parts.append("[其他消息]")
    return "".join(parts), kinds, faces, images


def _incoming(event: object, privates: set[str], groups: set[str],
              *, voice_text: str | None = None,
              image_ids: list[str] | None = None) -> dict | None:
    if not isinstance(event, dict) or event.get("post_type") != "message":
        return None
    scope = event.get("message_type")
    if scope not in ("private", "group"):
        return None
    user = _qq_id(event.get("user_id"))
    self_id = _qq_id(event.get("self_id"))
    group = _qq_id(event.get("group_id")) if scope == "group" else None
    message_id = _message_id(event.get("message_id"))
    if user is None or message_id is None or user == self_id:
        return None
    target = user if scope == "private" else group
    if target is None or target not in (privates if scope == "private" else groups):
        return None
    body, kinds, faces, images = _body(
        event.get("message", event.get("raw_message")), voice_text=voice_text,
        image_ids=image_ids)
    if not body.strip():
        return None
    sender = event.get("sender")
    name = ""
    if isinstance(sender, dict):
        for key in ("card", "nickname"):
            value = sender.get(key)
            if isinstance(value, str) and value.strip():
                name = " ".join(value.split())[:80]
                break
    raw_time = event.get("time")
    try:
        stamp = (datetime.fromtimestamp(raw_time, timezone.utc)
                 if isinstance(raw_time, (int, float)) and not isinstance(raw_time, bool)
                 else datetime.now(timezone.utc))
    except (OverflowError, ValueError, OSError):
        stamp = datetime.now(timezone.utc)
    conversation = f"{scope}:{target}"
    who = name or user
    # Keep the source and quote marker legible. Group messages retain their room
    # number; private ones retain the account -- the experience page turns this
    # marker into `[QQ·群]` / `[QQ·私]` and expands it to the room on click, and
    # the private room (who it was) only exists here.
    origin_label = (f"QQ私聊:{user}" if scope == "private" else f"QQ群{group}")
    meta = {"conversation": conversation, "message_id": message_id,
            "user_id": user, "segment_types": kinds}
    # Trusted transport body, separate from display names, quote markers and media.
    if 'reply' in kinds and isinstance(event.get('message'), list):
        replies = [_message_id((part.get('data') or {}).get('id')) for part in event['message']
                   if isinstance(part, dict) and part.get('type') == 'reply' and isinstance(part.get('data'), dict)]
        if len(replies) == 1 and replies[0] is not None:
            meta['reply_to_message_id'] = replies[0]
    if kinds == ["text"] and len(body) <= 64:
        meta["plain_text"] = body
    if scope == "group" and isinstance(event.get("message"), list):
        # OneBot's at segment names the addressee. Keep it separate from the
        # author and message text so the Agent does not answer for a member.
        targets: list[str] = []
        for part in event["message"]:
            if not isinstance(part, dict) or part.get("type") != "at":
                continue
            data = part.get("data")
            if not isinstance(data, dict):
                continue
            target_id = ("all" if data.get("qq") == "all" else
                         _qq_id(data.get("qq")))
            if target_id and target_id not in targets:
                targets.append(target_id)
        if targets:
            meta["mention_targets"] = targets
            meta["mentions_self"] = self_id in targets if self_id else None
    # Body contains media markers as text, so retain whether the sender wrote
    # actual words. The reply gate must not mistake an image marker for a prompt.
    if isinstance(event.get("message"), list):
        meta["has_user_text"] = any(
            isinstance(part, dict) and part.get("type") == "text"
            and isinstance(part.get("data"), dict)
            and bool(str(part["data"].get("text", "")).strip())
            for part in event["message"])
    else:
        meta["has_user_text"] = bool(str(event.get("message", "")).strip())
    if faces:
        meta["face_ids"] = faces
    if images:
        meta["image_kinds"] = images
    if image_ids:
        meta["media_ids"] = [one for one in image_ids if one]
    if "record" in kinds and voice_text is not None:
        meta["voice_transcription"] = "ok" if voice_text else "unavailable"
    if group is not None:
        meta["group_id"] = group
    if self_id is not None:
        meta["self_id"] = self_id
    draft = {
        "type": "message", "ts": stamp.isoformat(timespec="seconds"),
        "source": "qq", "origin": "external",
        "text": f"[{origin_label}·消息#{message_id}] {who}：{body}",
        "sender_key": f"qq:{user}", "meta": meta,
        "dedup_key": f"qq:{self_id or 'unknown'}:{conversation}:{message_id}",
    }
    if image_ids:
        draft["blobs"] = [f"plugin-media:core-infinity-qq:{conversation}:{one}"
                          for one in image_ids if one]
    return draft


class _IngressOrder:
    """Bounded per-conversation receipt order, independent of media completion.

    Never hold a lock while calling the host: push_event may synchronously
    marshal to the console loop. A single drainer per lane prevents a later
    completion from publishing ahead of an earlier, still-publishing item.
    Tickets belong to this instance; old callbacks cannot enter a new owner.
    """

    def __init__(self, emit, log, limit=256, start_thread=None):
        _require_legacy_runtime()
        self.emit, self.log, self.limit = emit, log, limit
        self.start_thread = start_thread
        self.lock = Lock()
        self.lanes = {}
        self.draining = set()
        # Retain Timer identities until native thread exit, including popped tickets.
        self.timers = set()
        self.pending = 0
        self.closed = False

    def reserve(self, scope):
        with self.lock:
            if self.closed or self.pending >= self.limit:
                return None
            ticket = {"owner": self, "scope": scope, "ready": False,
                      "live": True, "draft": None}
            self.lanes.setdefault(scope, []).append(ticket)
            self.pending += 1
            return ticket

    def watch(self, ticket, fallback, seconds=120):
        # A stalled media worker must not pin later text indefinitely. This
        # only expires publication, not the worker or its resource semaphore.
        if ticket is None:
            return
        def expire():
            if self.finish(ticket, fallback):
                self.log("QQ 媒体准备超时；保留原消息占位继续，迟到结果不重放")
        timer = Timer(seconds, expire)
        timer.daemon = True
        with self.lock:
            if self.closed or not ticket["live"] or ticket["ready"]:
                return
            self.timers = {t for t in self.timers if t.is_alive()}
            ticket["timer"] = timer
            self.timers.add(timer)
            # Serialize start against close; no registered-but-unstarted race.
            try:
                if callable(self.start_thread):
                    self.start_thread(timer,stop=timer.cancel)
                else:timer.start()
            except BaseException:
                self.timers.discard(timer)
                ticket.pop("timer", None)
                raise

    def finish(self, ticket, draft):
        if ticket is None:
            return False
        scope = ticket["scope"]
        with self.lock:
            if (self.closed or ticket["owner"] is not self
                    or not ticket["live"] or ticket["ready"]):
                return False
            ticket["ready"], ticket["draft"] = True, draft
            timer = ticket.pop("timer", None)
            if timer is not None:
                timer.cancel()
            if scope in self.draining:
                return True
            self.draining.add(scope)
        while True:
            with self.lock:
                lane = self.lanes.get(scope)
                if self.closed or not lane or not lane[0]["ready"]:
                    self.draining.discard(scope)
                    if not lane:
                        self.lanes.pop(scope, None)
                    return True
                head = lane.pop(0)
                head["live"] = False
                self.pending -= 1
            if head["draft"] is not None:
                try:
                    self.emit(head["draft"])
                except Exception as exc:
                    # Delivery may already have escaped; never replay it.
                    # Continue releasing this lane, but make the failure visible.
                    self.log(f"QQ 入站投递失败（未自动重放）：{type(exc).__name__}")

    def close(self):
        with self.lock:
            self.closed = True
            for timer in self.timers:
                timer.cancel()
            for lane in self.lanes.values():
                for ticket in lane:
                    ticket["live"] = False
                    timer = ticket.pop("timer", None)
                    if timer is not None:
                        timer.cancel()
            self.lanes.clear()
            self.pending = 0
            return tuple(self.timers)


_STOP_BUDGET_SECONDS = 3.0  # one budget for only this instance's retained workers


def _require_legacy_runtime():
    # Same sticky process-local latch even for direct file-loaded entrypoints.
    # No ctx/host label or alternate package root may grant an exception.
    from telyincore.plugins import _require_unconfined_lifecycle
    _require_unconfined_lifecycle()


def create(ctx):
    _require_legacy_runtime()
    from telyincore.plugins import PluginSetupRequired
    config_path = Path(ctx.get("qq_config_path") or (Path(ctx["packageDir"]) / "config.json"))
    try:
        settings = json.loads(config_path.read_bytes())
    except FileNotFoundError:
        settings = {}
    if isinstance(settings, dict) and not (
        _ids(settings.get("privates", []), "privates")
        or _ids(settings.get("groups", []), "groups")
    ):
        raise PluginSetupRequired("请到 QQ 插件页配置连接和允许的私聊或群聊。")

    managed_owner = callable(ctx.get("owner_qq_route"))

    class QQ:
        def __init__(self):
            _require_legacy_runtime()
            from contextvars import ContextVar
            from telyincore.async_tools import OwnedBlockingIO
            self._tool_io = OwnedBlockingIO()
            self._tool_frame = ContextVar('qq_owned_tool_frame', default=None)
            self._account_lock = Lock()
            self._linux_account_config = None
            self._observed_account = None  # untrusted protocol claim, NOT ownership
            self._collection_lock = Lock()
            self._sticker_vision = ContextVar("qq_sticker_vision", default=None)
            self._account_conflict = Event()  # sticky even after clean stop/start
            self._run_lock = Lock()
            self._started = False
            self.client = None
            self.tts_client = None
            self._tts_owner = uuid.uuid4().hex
            self._tts_condition = Condition()
            self._tts_order = []
            self.privates: set[str] = set()
            self.groups: set[str] = set()
            self._preferred_owner_qq = ""
            self.sharing = None
            self.transcribe_voice = False
            self.tts_url = ""
            self.tts_voice = "default"
            self.tts_send_mode = "on_demand"
            self.media_client = None
            self.asr_model_dir = ""
            self.asr_language = "auto"
            self.asr_use_itn = True
            self.asr_threads = 3
            self.backfill_minutes = 0
            self._asr_lock = Lock()
            self._asr_recognizer = None
            self._asr_vad = None
            self.ws_url = ""
            self.ws_headers: dict[str, str] = {}
            self._stopping = Event()
            self._thread: Thread | None = None
            self._loop: asyncio.AbstractEventLoop | None = None
            self._task: asyncio.Task | None = None
            self.connected = False
            self._ingress_order = _IngressOrder(self._emit_current, self._log, start_thread=ctx.get('start_background_thread'))
            self._voice_executor: ThreadPoolExecutor | None = None
            self._voice_slots = Semaphore(16)
            self._voice_lock = Lock()
            self._voice_pending: set[str] = set()
            from functools import lru_cache
            self._cached_image_digest = lru_cache(maxsize=128)(self._image_digest)
            self._status_slot = Semaphore(2)
            self._reply_lock = Lock()
            self._recent_messages: OrderedDict[tuple[str, str], None] = OrderedDict()
            self._recalled: OrderedDict[tuple[str, str], None] = OrderedDict()
            self.reaction_window_seconds = 3
            from telyincore.plugin_media_scope import qq_media_paths
            self.media_dir, self.outbox_dir = qq_media_paths(ctx)
            from telyincore.voice_corpus import VoiceCorpus
            self.voice_corpus = VoiceCorpus(Path(ctx["root"]), Path(
                ctx.get("qq_config_path") or (Path(ctx["packageDir"]) / "config.json")))
            self._corpus_plugin_hash = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()

        def start(self):
            _require_legacy_runtime()
            if self._account_conflict.is_set():
                raise RuntimeError("QQ account claim changed; instance fenced")
            if not self._run_lock.acquire(blocking=False):
                raise RuntimeError("QQ lifecycle operation busy")
            try:
                if self._started or any(value is not None for value in (
                        self._thread, self._voice_executor, self.client, self.media_client, self.tts_client)):
                    raise RuntimeError("QQ instance must finish stop before restart")
                self._started = True  # partial start failures require explicit cleanup too
                return self._start()
            finally:
                self._run_lock.release()

        def _start(self):
            _require_legacy_runtime()
            config_path = Path(ctx.get("qq_config_path") or (Path(ctx["packageDir"]) / "config.json"))
            self._voice_config_path = config_path
            config_snapshot = config_path.read_bytes()
            settings = json.loads(config_snapshot)
            (url, token, self.ws_url, ws_token, self.privates, self.groups,
             self._preferred_owner_qq, self.transcribe_voice, self.tts_url,
             self.asr_model_dir, self.backfill_minutes, self.tts_voice,
             self.asr_language, self.asr_use_itn, self.asr_threads,
             self.tts_send_mode) = _config(
                Path(ctx.get("qq_config_path") or (Path(ctx["packageDir"]) / "config.json")),
                model_root=Path(ctx["root"]) / "models", snapshot=settings, managed_owner=managed_owner
            )
            if callable(ctx.get('linux_qq_check')):
                self._linux_account_config = settings
                self._observed_account = _qq_id(settings.get('linuxAccount'))
                if self._observed_account is None:
                    raise ValueError("Linux SL 缺少已配置的登录 QQ 号")
            bot_id = ctx.get("bot_id")
            if bot_id is not None:
                owner = settings.get("ownerBot") or (bot_id if ctx.get("qq_config_path")
                                                        else ctx.get("baseline_bot"))
                if not isinstance(owner, str) or not owner or bot_id != owner:
                    raise RuntimeError("共享 QQ 配置只允许所属 Bot 加载，避免多 Bot 同号重复接线")
            window = settings.get("reactionWindowSeconds", 3)
            if type(window) is not int or not 0 <= window <= 10:
                raise ValueError("消息反应窗口必须是 0 到 10 秒的整数")
            self.reaction_window_seconds = window
            if self.ws_url and not callable(ctx.get("push_event")):
                raise ValueError("QQ 入站需要宿主的 push_event 事件入口")
            headers = {"Authorization": f"Bearer {token}"} if token else {}
            self.client = httpx.Client(base_url=url, headers=headers,
                                       timeout=10.0, trust_env=False)
            self.media_client = httpx.Client(
                timeout=httpx.Timeout(15.0, connect=5.0), trust_env=False,
                follow_redirects=False)
            if self.tts_url:
                self.tts_client = httpx.Client(
                    base_url=self.tts_url,
                    timeout=httpx.Timeout(180.0, connect=5.0), trust_env=False)
            if _sharing_enabled(settings, managed_owner=managed_owner):
                from telyincore.sharing_native import NativeSharing
                try:
                    self.sharing = NativeSharing(self, ctx, config_path, config_snapshot=config_snapshot)
                except (OSError, ValueError) as exc:
                    if not managed_owner:
                        raise
                    self._log(f"主动分享初始化失败，QQ 收发继续运行：{exc}")
            self.ws_headers = {"Authorization": f"Bearer {ws_token}"} if ws_token else {}
            self._ingress_order = _IngressOrder(self._emit_current, self._log, start_thread=ctx.get('start_background_thread'))
            self._stopping.clear()
            self._voice_executor = ThreadPoolExecutor(
                max_workers=2, thread_name_prefix="qq-media")
            if self.ws_url:
                self._thread = Thread(target=self._thread_main, name="qq-onebot-ws", daemon=True)
                start_thread=ctx.get('start_background_thread')
                if callable(start_thread):
                    self._thread_receipt=start_thread(self._thread,stop=self._signal_background_stop)
                else:self._thread.start()

        def _signal_background_stop(self):
            # Nonblocking phase one; native join and client close stay in _stop.
            self._stopping.set()
            loop,task=self._loop,self._task
            if loop is not None and task is not None:
                try:loop.call_soon_threadsafe(task.cancel)
                except RuntimeError:pass

        def _submit_background(self, function, *args):
            observe=ctx.get('start_background_work')
            factory=lambda:self._voice_executor.submit(function,*args)
            return observe(factory) if callable(observe) else factory()

        def stop(self):
            self._tool_io.assert_idle()
            if not self._run_lock.acquire(blocking=False):
                raise RuntimeError("QQ lifecycle operation busy")
            try:
                return self._stop()
            finally:
                self._run_lock.release()

        def _stop(self):
            self._stopping.set()
            if self.sharing is not None:
                self.sharing.close()
                self.sharing = None
            ingress_threads = self._ingress_order.close()
            self._cancel_tts()
            with self._reply_lock:
                self._recent_messages.clear()
            executor = self._voice_executor
            if executor is not None:
                # shutdown rejects new submits before taking the final owned set.
                # CPython executor threads only, never a host-wide thread census.
                executor.shutdown(wait=False, cancel_futures=True)
                workers = getattr(executor, "_threads", None)
                if type(workers) is not set or not all(isinstance(t, Thread) for t in workers):
                    raise RuntimeError("QQ media worker identities unavailable; stop incomplete")
                media_threads = tuple(workers)
            else:
                media_threads = ()
            loop, task = self._loop, self._task
            if loop is not None and task is not None:
                try:
                    loop.call_soon_threadsafe(task.cancel)
                except RuntimeError:
                    pass  # native thread evidence below still decides completion
            thread = self._thread
            if thread is not None and not isinstance(thread, Thread):
                raise RuntimeError("QQ websocket worker identity unavailable; stop incomplete")
            threads = tuple(dict.fromkeys((() if thread is None else (thread,)) + media_threads + ingress_threads))
            deadline = time.monotonic() + _STOP_BUDGET_SECONDS
            for worker in threads:
                if worker is current_thread():
                    raise RuntimeError("QQ worker cannot certify its own retirement")
                if worker.ident is not None or worker.is_alive():
                    worker.join(timeout=max(0, deadline - time.monotonic()))
            if any(worker.is_alive() for worker in threads):
                # Keep executor/thread AND clients. Loader retains this failed row;
                # a later stop may retry once the actual workers have exited.
                raise RuntimeError("QQ workers still alive; stop incomplete, restart denied")
            receipt = getattr(self, '_thread_receipt', None)
            if receipt is not None:
                if not isinstance(receipt, Future):
                    raise RuntimeError('QQ native receipt identity unavailable')
                receipt.result(timeout=max(0, deadline - time.monotonic()))
            self._thread = None
            self._voice_executor = None
            self._loop = self._task = None
            self.connected = False
            for name in ("client", "media_client", "tts_client"):
                client = getattr(self, name)
                if client is not None:
                    client.close()  # retain reference on failure; do not report closed
                    setattr(self, name, None)
            self._started = False

        def _log(self, line: str):
            note = ctx.get("log")
            if callable(note):
                note(line)

        def _observe_account(self, value) -> bool:
            # Denial-only cross-check of observed claims. Missing legacy fields
            # remain unknown, never a verified account or a release capability.
            account = _qq_id(value)
            with self._account_lock:
                if self._account_conflict.is_set():
                    return False
                if account is None:
                    return True
                if self._observed_account is None:
                    self._observed_account = account
                elif account != self._observed_account:
                    self._account_conflict.set()
                else:
                    return True
            if self._account_conflict.is_set():
                self.connected = False
                self._ingress_order.close()  # no late media/timer publication
                self._log("QQ 账号声明发生变化；本实例已封口，需重新核验接入")
                return False
            return True

        def _notify_sharing(self, hook: str, event: dict) -> None:
            if self._account_conflict.is_set():
                return
            if self.sharing is not None:
                if hook == "sharing_ingress":
                    self.sharing.capture(event)
                elif hook == "sharing_recall":
                    self.sharing.recall(event)
            # Host-only, default-off integration seam. Not a model tool or sender.
            _require_legacy_runtime()
            if ctx.get("sharing_preview_enabled") is not True:
                return
            callback = ctx.get(hook)
            if not callable(callback):
                return
            try:
                snapshot = json.loads(json.dumps(event, ensure_ascii=False))
                callback(snapshot)
            except Exception:
                try:
                    self._log("分享预览宿主回调失败，已跳过")
                except Exception:
                    pass

        def _group_owner(self, events, *, require_words=False):
            from qq_conversation_privacy import group_owner_sender
            return group_owner_sender(events, lambda sender: sender[3:] in ctx['owner_qq_route']()['accounts'],
                                      require_words=require_words)

        def event_requires_isolation(self, event):
            # A nearby group member must never lend or borrow this sender's authority.
            return self._group_owner((event,)) is not None

        def _answer_status(self, draft):
            from qq_conversation_privacy import owner_message_batch
            from qq_status_command import claim, status_text
            from telyincore.plugin_media_scope import qq_media_paths
            meta = draft['meta']
            scope, target = meta['conversation'].split(':', 1)
            def authorized():
                return (not self._stopping.is_set() and self.event_is_current(draft)
                        and self._observed_account == meta.get('self_id')
                        and scope == 'private' and owner_message_batch((draft,), ctx.get('owner_qq_route')))
            if not authorized():
                return
            field, target = self._target(scope, target)
            if str((self._post('get_login_info', {}).get('data') or {}).get('user_id')) != meta['self_id']:
                return
            try:
                text = status_text(self._post)
            except Exception as exc:
                self._log(f'SnowLuma 状态详情暂不可读：{type(exc).__name__}')
                text = 'SnowLuma 已收到查询，当前无法读取详细状态。'
            if not authorized() or not claim(qq_media_paths(ctx)[0].parent, draft['dedup_key']):
                return
            self._target(scope, target)
            if not authorized():
                return
            result = self._post('send_private_msg', {field: int(target), 'message': [
                {'type':'reply','data':{'id':meta['message_id']}},
                {'type':'text','data':{'text':text}}]})
            self._receipt(result, scope, target)
            self._log('已回复 owner 的 SnowLuma 状态查询（' + scope + '）')

        def _status_done(self, job):
            self._status_slot.release()
            if not job.cancelled() and job.exception() is not None:
                self._log('SnowLuma 状态查询未完成：' + type(job.exception()).__name__)

        def accept(self, raw: str | bytes, *, from_history=False) -> bool:
            _require_legacy_runtime()
            if self._account_conflict.is_set():
                return False
            try:
                event = json.loads(raw)
            except (ValueError, TypeError):
                return False
            if isinstance(event, dict) and event.get("post_type") == "notice":
                if event.get("sub_type") != "poke":
                    return self._accept_recall(event)
                draft = poke_notice(event, self.privates, self.groups, _qq_id)
                if draft is None or not self._observe_account(event["self_id"]):
                    return False
                ticket = self._ingress_order.reserve(draft["meta"]["conversation"])
                if ticket is None:
                    return False
                self._publish_current(draft, ticket)
                return True
            draft = _incoming(event, self.privates, self.groups)
            if draft is None:
                return False
            if not self._observe_account(event.get("self_id")):
                return False
            from qq_conversation_privacy import owner_message_batch
            text, _, _, _ = _body(event.get("message", event.get("raw_message")))
            # A reserved command is accepted only as a standalone DM from the verified owner.
            # QQ mentions and reply segments cannot confer command privileges.
            parts = event.get('message')
            command = (''.join(part['data']['text'] for part in parts
                               if isinstance(part, dict) and part.get('type') == 'text'
                               and isinstance(part.get('data'), dict) and isinstance(part['data'].get('text'), str))
                       if isinstance(parts, list) else text).strip().casefold()
            if re.match(r'^#sl(?:$|\s)', command):
                if (command != '#sl' or text.strip().casefold() != command
                        or event.get('message_type') != 'private'
                        or not owner_message_batch((draft,), ctx.get('owner_qq_route'))):
                    return False
                if from_history or self._stopping.is_set() or not self.event_is_current(draft):
                    return False
                if not self._status_slot.acquire(blocking=False):
                    self._log('SnowLuma 状态查询正在处理，请稍后再试')
                    return False
                try:
                    self._submit_background(self._answer_status, draft).add_done_callback(self._status_done)
                except Exception:
                    self._status_slot.release()
                    raise
                return True
            if not self.event_is_current(draft):
                return True
            ticket = self._ingress_order.reserve(draft["meta"]["conversation"])
            if ticket is None:
                self._log("QQ 入站等待队列已满或已停止；本条未接收")
                return False
            with self._reply_lock:
                key = (draft["meta"]["conversation"], draft["meta"]["message_id"])
                self._recent_messages[key] = None
                self._recent_messages.move_to_end(key)
                if len(self._recent_messages) > 512:
                    self._recent_messages.popitem(last=False)
            if self.transcribe_voice and "record" in draft["meta"]["segment_types"] and self._asr_active():
                return self._queue_voice(event, draft, ticket)
            message = event.get("message")
            has_image_file = (isinstance(message, list) and any(
                isinstance(part, dict) and part.get("type") == "file"
                and isinstance(part.get("data"), dict) and _image_file(part["data"])
                for part in message))
            if "image" in draft["meta"]["segment_types"] or has_image_file:
                return self._queue_images(event, draft, ticket)
            self._publish_current(draft, ticket)
            self._notify_sharing("sharing_ingress", event)
            return True

        def _accept_recall(self, event: dict) -> bool:
            kind = event.get("notice_type")
            if kind not in ("group_recall", "friend_recall"):
                return False
            if not self._observe_account(event.get("self_id")):
                return False
            scope = "group" if kind == "group_recall" else "private"
            target = _qq_id(event.get("group_id" if scope == "group" else "user_id"))
            message = _message_id(event.get("message_id"))
            allowed = self.groups if scope == "group" else self.privates
            if target is None or target not in allowed or message is None:
                return False
            key = (f"{scope}:{target}", message)
            with self._reply_lock:
                self._recalled[key] = None
                self._recalled.move_to_end(key)
                self._recent_messages.pop(key, None)
                if len(self._recalled) > 2048:
                    self._recalled.popitem(last=False)
            self._log("QQ 已记录消息撤回；阻止尚未发送的对应回复")
            self._notify_sharing("sharing_recall", event)
            return True

        def event_is_current(self, event: object) -> bool:
            source = event.get("source") if isinstance(event, dict) else getattr(
                event, "source", None)
            if source != "qq":
                return True
            if self._account_conflict.is_set():
                return False
            meta = event.get("meta") if isinstance(event, dict) else getattr(event, "meta", None)
            if not isinstance(meta, dict):
                return True
            key = (str(meta.get("conversation", "")), str(meta.get("message_id", "")))
            with self._reply_lock:
                return key not in self._recalled

        def _guard_recalled_batch(self) -> None:
            active = self._active_tool_events()
            if callable(active) and any(not self.event_is_current(e) for e in active()):
                raise ValueError("本批 QQ 消息已撤回，停止发送；请等待新的消息")

        def _guard_current_target(self, scope: str, target: str) -> None:
            """Do not let a current recipient's turn address another QQ room.

            A local console operator may intentionally address an allowlisted
            QQ target. An autonomous turn has no current recipient and may use
            only the separately configured proactive private target.
            """
            active = self._active_tool_events()
            if not callable(active):
                return  # Standalone/manual fixture has no runtime turn.
            batch = tuple(active() or ())
            external = [item for item in batch if (
                item.get("origin") if isinstance(item, dict)
                else getattr(item, "origin", None)) == "external"]
            if not external:
                if (scope, target) != ("private", self.proactive_target):
                    raise ValueError("自主轮次只能发送给已配置的主动私聊目标")
                return
            sources = {item.get("source") if isinstance(item, dict) else getattr(
                item, "source", None) for item in external}
            if sources == {"console"}:
                return  # The local console is the operator's entrance.
            rooms: set[tuple[str, str]] = set()
            for item in external:
                source = item.get("source") if isinstance(item, dict) else getattr(
                    item, "source", None)
                if source == "console":
                    raise ValueError("混合来源不能发送 QQ 消息")
                if source != "qq":
                    raise ValueError("非 QQ 来源不能发送 QQ 消息")
                meta = item.get("meta") if isinstance(item, dict) else getattr(
                    item, "meta", None)
                conversation = meta.get("conversation") if isinstance(meta, dict) else None
                if not isinstance(conversation, str):
                    raise ValueError("当前 QQ 会话缺少可信目标")
                room, sep, recipient = conversation.partition(":")
                if not sep or room not in ("private", "group") or _qq_id(recipient) != recipient:
                    raise ValueError("当前 QQ 会话目标无效")
                rooms.add((room, recipient))
            if poke_only_addressed(external, scope, target) is False:
                raise ValueError("本轮戳一戳未指向本 Bot")
            if rooms != {(scope, target)}:
                raise ValueError("本轮 QQ 消息只能发送回当前会话")

        def _current_group_ids(self, target: str) -> set[str] | None:
            """Trusted message IDs for a live group turn; None outside one."""
            active = self._active_tool_events()
            if not callable(active):
                return None
            ids: set[str] = set()
            seen_group = False
            for item in tuple(active() or ()):
                source = item.get("source") if isinstance(item, dict) else getattr(
                    item, "source", None)
                origin = item.get("origin") if isinstance(item, dict) else getattr(
                    item, "origin", None)
                if source != "qq" or origin != "external":
                    continue
                meta = item.get("meta") if isinstance(item, dict) else getattr(
                    item, "meta", None)
                if isinstance(meta, dict) and meta.get("conversation") == f"group:{target}":
                    seen_group = True
                    message_id = _message_id(meta.get("message_id"))
                    if message_id is not None:
                        ids.add(message_id)
            return ids if seen_group else None

        def _publish_current(self, draft: dict, ticket=None) -> None:
            if ticket is None:
                ticket = self._ingress_order.reserve(draft["meta"]["conversation"])
            # Use the ticket's original owner, even after stop/start.
            if ticket is not None:
                ticket["owner"].finish(ticket, draft)

        def _emit_current(self, draft: dict) -> None:
            _require_legacy_runtime()
            if self._stopping.is_set():
                return
            if self.event_is_current(draft):
                draft["meta"]["reaction_window_ms"] = self.reaction_window_seconds * 1000
                ctx["push_event"](draft)

        def _queue_images(self, event: dict, draft: dict, ticket=None) -> bool:
            if ticket is not None:
                ticket["owner"].watch(ticket, draft)
            key = draft["dedup_key"]
            with self._voice_lock:
                duplicate = key in self._voice_pending
                unavailable = (not duplicate and (self._voice_executor is None
                               or not self._voice_slots.acquire(blocking=False)))
                if not duplicate and not unavailable:
                    self._voice_pending.add(key)
                    try:
                        future = self._submit_background(self._attach_images, event, draft)
                    except Exception:
                        self._voice_pending.discard(key)
                        self._voice_slots.release()
                        unavailable = True
            if duplicate:
                if ticket is not None:
                    ticket["owner"].finish(ticket, None)
                return True
            if unavailable:
                self._publish_current(draft, ticket)
                return True
            future.add_done_callback(lambda finished: self._images_done(finished, key, draft, ticket))
            return True

        def _images_done(self, future: Future, key: str, fallback: dict, ticket=None) -> None:
            with self._voice_lock:
                self._voice_pending.discard(key)
                self._voice_slots.release()
            if self._stopping.is_set():
                return
            try:
                draft = future.result()
            except Exception as exc:
                self._log(f"QQ 图片暂不可用：{type(exc).__name__}")
                draft = fallback
            self._publish_current(draft, ticket)

        def _attach_images(self, event: dict, draft: dict,
                           voice_text: str | None = None) -> dict:
            message = event.get("message")
            if not isinstance(message, list):
                return draft
            image_ids: list[str] = []
            for segment in message:
                if (not isinstance(segment, dict)
                        or segment.get("type") not in ("image", "file")):
                    continue
                data = segment.get("data")
                if not isinstance(data, dict) or (segment["type"] == "file" and not _image_file(data)):
                    continue
                if len(image_ids) >= MAX_INCOMING_IMAGES:
                    image_ids.append("")
                    continue
                try:
                    if segment["type"] == "file":
                        image_ids.append(self._save_file_image(
                            data, draft["meta"]["conversation"],
                            draft["meta"]["user_id"]))
                    else:
                        image_ids.append(self._save_image(
                            data.get("url"), draft["meta"]["conversation"],
                            media_kind=_incoming_image_kind(data)))
                except Exception as exc:
                    self._log(f"QQ 图片缓存暂不可用：{type(exc).__name__}")
                    image_ids.append("")
            if not any(image_ids):
                return draft
            received = _incoming(event, self.privates, self.groups,
                                 image_ids=image_ids, voice_text=voice_text) or draft
            conversation = received["meta"]["conversation"]
            for media_id in dict.fromkeys(filter(None, image_ids)):
                try:
                    raw, _ = self._saved_image(media_id, conversation=conversation)
                    known = self._sticker_store().recognition(raw)
                except (OSError, ValueError, RuntimeError) as exc:
                    self._log(f"旧图识别暂不可用：{type(exc).__name__}")
                    continue
                if known:
                    ref = f"plugin-media:core-infinity-qq:{conversation}:{media_id}"
                    received["blobs"] = [b for b in received.get("blobs", []) if b != ref]
                    # Keep the current conversation's handle available for another look.
                    received["text"] += (f"\n[旧图 {media_id} · 上次的画面描述，可重新查看] "
                                         + json.dumps(known, ensure_ascii=False))
            return received

        def _save_file_image(self, data: dict, conversation: str, user_id: str) -> str:
            _require_legacy_runtime()
            scope, separator, target = conversation.partition(":")
            if not separator or scope not in ("private", "group") or _qq_id(target) != target:
                raise ValueError("QQ 图片文件会话无效")
            file_id = data.get("file_id")
            file_hash = data.get("file_hash", "")
            if (not isinstance(file_id, str) or not 1 <= len(file_id) <= 512
                    or not isinstance(file_hash, str) or len(file_hash) > 512):
                raise ValueError("QQ 图片文件标识无效")
            if scope == "private":
                result = self._post("get_private_file_url", {
                    "user_id": int(user_id), "file_id": file_id, "file_hash": file_hash})
            else:
                busid = data.get("busid", data.get("file_bus_id", 102))
                if isinstance(busid, str) and busid.isascii() and busid.isdecimal():
                    busid = int(busid)
                if type(busid) is not int or not 0 <= busid <= 2**31 - 1:
                    raise ValueError("QQ 群文件 busid 无效")
                result = self._post("get_group_file_url", {
                    "group_id": int(target), "file_id": file_id, "busid": busid})
            info = result.get("data")
            url = info.get("url") if isinstance(info, dict) else None
            return self._save_image(url, conversation, media_kind="file_image")

        def _save_image(self, url: object, conversation: str,
                        *, media_kind: str | None = None) -> str:
            _require_legacy_runtime()
            if self.media_client is None:
                raise RuntimeError("QQ 插件尚未启动")
            url = _trusted_file_url(url) if media_kind == "file_image" else _trusted_image_url(url)
            with self.media_client.stream("GET", url) as response:
                response.raise_for_status()
                if response.headers.get("content-length") and int(
                        response.headers["content-length"]) > 8 * 1024 * 1024:
                    raise ValueError("QQ 图片超过 8 MB")
                chunks, size = [], 0
                for chunk in response.iter_bytes():
                    size += len(chunk)
                    if size > 8 * 1024 * 1024:
                        raise ValueError("QQ 图片超过 8 MB")
                    chunks.append(chunk)
            raw = b"".join(chunks)
            extension, mime = _image_format(raw)
            from PIL import Image
            with warnings.catch_warnings():
                warnings.simplefilter("error", Image.DecompressionBombWarning)
                with Image.open(io.BytesIO(raw), formats=("PNG", "JPEG", "GIF", "WEBP")) as picture:
                    if (picture.width > 4096 or picture.height > 4096
                            or picture.width * picture.height > 16_000_000):
                        raise ValueError("QQ 图片尺寸超过限制")
                    picture.verify()
            media_id = _cached_media_id(conversation, raw, media_kind)
            from telyincore.plugin_media_scope import qq_media_paths
            qq_media_paths(ctx)  # reject a redirected per-Bot media directory
            self.media_dir.mkdir(parents=True, exist_ok=True)
            media = self.media_dir / f"{media_id}.{extension}"
            info = self.media_dir / f"{media_id}.json"
            if not media.exists():
                _write_cached_file(media, raw)
            metadata = {"conversation": conversation, "extension": extension, "mime": mime,
                        "size": len(raw), "media_kind": media_kind}
            _write_cached_file(info, json.dumps(metadata).encode("utf-8"))
            return media_id

        def _queue_voice(self, event: dict, draft: dict, ticket=None) -> bool:
            key = draft["dedup_key"]
            if ticket is not None:
                ticket["owner"].watch(ticket, draft)
            unavailable = False
            with self._voice_lock:
                duplicate = key in self._voice_pending
                if duplicate:
                    pass
                elif self._voice_executor is None or not self._voice_slots.acquire(blocking=False):
                    unavailable = True
                else:
                    self._voice_pending.add(key)
                    try:
                        future = self._submit_background(
                            self._transcribe_voice, draft["meta"]["message_id"], event)
                    except Exception:
                        self._voice_pending.discard(key)
                        self._voice_slots.release()
                        unavailable = True
            if duplicate:
                if ticket is not None:
                    ticket["owner"].finish(ticket, None)
                return True
            if unavailable:
                fallback = _incoming(event, self.privates, self.groups, voice_text="")
                if fallback is not None:
                    fallback["meta"]["voice_transcription_source"] = "unavailable"
                    self._publish_current(fallback, ticket)
                elif ticket is not None:
                    ticket["owner"].finish(ticket, None)
                return True
            future.add_done_callback(lambda finished: self._ordered_voice_done(
                finished, key, event, ticket))
            return True

        def _ordered_voice_done(self, future, key, event, ticket):
            try:
                self._voice_done(future, key, event, ticket=ticket)
            finally:
                # Silence / invalidated message / callback failure must not
                # strand following text behind an uncompleted media slot.
                if ticket is not None:
                    ticket["owner"].finish(ticket, None)

        def _transcribe_voice(self, message_id: str, event: dict) -> tuple[str, str]:
            _require_legacy_runtime()
            if self.asr_model_dir:
                try:
                    transcript = self._transcribe_local(event)
                    if transcript:
                        # A VAD-positive recording can still decode to only punctuation.
                        # This is empty content, not proof that the audio had no speech.
                        if not any(char.isalnum() for char in transcript):
                            return "", "local_no_text"
                        return transcript[:4000], "local_sensevoice"
                except _NoSpeechDetected:
                    return "", "local_no_speech"
                except Exception as exc:
                    self._log(f"QQ 本地语音识别暂不可用：{type(exc).__name__}")
            result = self._post("fetch_ptt_text", {"message_id": int(message_id)})
            data = result.get("data")
            transcript = data.get("text") if isinstance(data, dict) else None
            text = transcript.strip()[:4000] if isinstance(transcript, str) else ""
            return text, "snowluma" if text else "unavailable"

        def _asr_session(self):
            from qq_voice_services import session_for
            path = getattr(self, "_voice_config_path", None)
            return session_for(path) if path else None

        def _asr_active(self):
            session = self._asr_session()
            return session is None or session.engine is not None

        def _transcribe_local(self, event: dict) -> str:
            _require_legacy_runtime()
            message = event.get("message")
            record = next((part.get("data", {}).get("file")
                           for part in message if isinstance(part, dict)
                           and part.get("type") == "record"
                           and isinstance(part.get("data"), dict)), None) if isinstance(message, list) else None
            if not isinstance(record, str) or not record:
                raise ValueError("QQ 语音事件没有文件标识")
            result = self._post("get_record", {"file": record, "out_format": "wav"})
            data = result.get("data")
            encoded = data.get("base64") if isinstance(data, dict) else None
            if not isinstance(encoded, str) or len(encoded) > 14 * 1024 * 1024:
                raise ValueError("QQ 语音音频缺失或超过大小限制")
            try:
                raw = base64.b64decode(encoded, validate=True)
            except binascii.Error:
                raise ValueError("QQ 语音音频不是有效 base64") from None
            if len(raw) > 10 * 1024 * 1024:
                raise ValueError("QQ 语音音频超过 10 MB")
            import numpy as np
            from scipy.signal import resample_poly
            with wave.open(io.BytesIO(raw), "rb") as wav:
                rate, channels, width = wav.getframerate(), wav.getnchannels(), wav.getsampwidth()
                if (rate < 8000 or rate > 48000 or channels not in (1, 2) or width != 2
                        or wav.getnframes() > rate * 30):
                    raise ValueError("QQ 语音音频格式或时长不受支持")
                samples = np.frombuffer(wav.readframes(wav.getnframes()), dtype="<i2")
            samples = samples.reshape(-1, channels).astype(np.float32).mean(axis=1) / 32768
            if rate != 16000:
                from math import gcd
                common = gcd(rate, 16000)
                samples = resample_poly(samples, 16000 // common, rate // common).astype(np.float32)
            session = self._asr_session()
            if session is not None:
                transcript = session.decode(samples)
                if transcript is None:
                    raise _NoSpeechDetected()
                return transcript
            with self._asr_lock:
                import sherpa_onnx
                model = Path(self.asr_model_dir)
                vad_path = model / "silero_vad.onnx"
                # Missing/broken optional VAD is a local failure, never proof
                # of silence. Preserve the existing SnowLuma fallback policy.
                if not vad_path.is_file():
                    raise RuntimeError("本地 VAD 模型不可用")
                if self._asr_vad is None:
                    config = sherpa_onnx.VadModelConfig()
                    config.silero_vad.model = str(vad_path)
                    config.silero_vad.threshold = 0.5
                    config.silero_vad.min_speech_duration = 0.1
                    config.silero_vad.min_silence_duration = 0.5
                    config.silero_vad.max_speech_duration = 30
                    config.sample_rate = 16000
                    config.num_threads = 1
                    config.provider = "cpu"
                    self._asr_vad = sherpa_onnx.VoiceActivityDetector(
                        config, buffer_size_in_seconds=31)
                vad = self._asr_vad
                speech = False
                vad.reset()
                try:
                    for offset in range(0, len(samples), 512):
                        frame = samples[offset:offset + 512]
                        if len(frame) < 512:
                            frame = np.pad(frame, (0, 512 - len(frame)))
                        vad.accept_waveform(frame)
                        if not vad.empty():
                            speech = True
                            break
                    if not speech:
                        vad.flush()
                        speech = not vad.empty()
                finally:
                    vad.reset()
                if not speech:
                    raise _NoSpeechDetected()
                # Gate only: never trim VAD segments or split the decoder input.
                # Keep quiet phonemes and short replies in their full context.
                if self._asr_recognizer is None:
                    import sherpa_onnx
                    model = Path(self.asr_model_dir)
                    self._asr_recognizer = sherpa_onnx.OfflineRecognizer.from_sense_voice(
                        model=str(model / "model.int8.onnx"),
                        tokens=str(model / "tokens.txt"), num_threads=self.asr_threads,
                        provider="cpu", sample_rate=16000, language=self.asr_language,
                        use_itn=self.asr_use_itn)
                stream = self._asr_recognizer.create_stream()
                stream.accept_waveform(16000, samples)
                self._asr_recognizer.decode_stream(stream)
                return stream.result.text.strip()

        def _voice_done(self, future: Future, key: str, event: dict, ticket=None) -> None:
            with self._voice_lock:
                self._voice_pending.discard(key)
                self._voice_slots.release()
            if self._stopping.is_set():
                return
            try:
                transcript, source = future.result()
            except Exception as exc:
                self._log(f"QQ 语音转写暂不可用：{type(exc).__name__}")
                transcript = ""
                source = "unavailable"
            if source in ("local_no_speech", "local_no_text"):
                # Do not publish a misleading unavailable-voice marker, or
                # drop accompanying text/images. Never mutate original event.
                event = dict(event)
                event["message"] = [part for part in event.get("message", [])
                                    if not (isinstance(part, dict)
                                            and part.get("type") == "record")]
            draft = _incoming(event, self.privates, self.groups, voice_text=transcript)
            if draft is not None and any(
                    kind in draft["meta"]["segment_types"] for kind in ("image", "file")):
                draft = self._attach_images(event, draft, voice_text=transcript)
            if draft is not None and not self._stopping.is_set():
                draft["meta"]["voice_transcription_source"] = source
                if ticket is None:
                    self._publish_current(draft)
                else:
                    self._publish_current(draft, ticket)

        def _thread_main(self):
            _require_legacy_runtime()
            try:
                asyncio.run(self._receive())
            except asyncio.CancelledError:
                pass
            finally:
                self.connected = False
                self._loop = None
                self._task = None

        def _backfill_recent(self) -> None:
            _require_legacy_runtime()
            login = self._post("get_login_info", {})
            data = login.get("data")
            self_id = _qq_id(data.get("user_id")) if isinstance(data, dict) else None
            if self_id is None:
                raise RuntimeError("QQ 历史补读无法确认当前账号")
            if not self._observe_account(self_id):
                raise RuntimeError("QQ account claim changed; backfill fenced")
            now = time.time()
            cutoff = now - self.backfill_minutes * 60
            recovered = 0
            for scope, targets, action, field in (
                ("private", self.privates, "get_friend_msg_history", "user_id"),
                ("group", self.groups, "get_group_msg_history", "group_id"),
            ):
                for target in targets:
                    try:
                        result = self._post(action, {field: int(target), "count": 30})
                        history = result.get("data")
                        rows = history.get("messages") if isinstance(history, dict) else None
                        if not isinstance(rows, list):
                            continue
                        for row in sorted((one for one in rows if isinstance(one, dict)),
                                          key=lambda one: one.get("time") or 0):
                            stamp = row.get("time")
                            if (not isinstance(stamp, (int, float)) or isinstance(stamp, bool)
                                    or not cutoff <= stamp <= now + 60
                                    or _qq_id(row.get("user_id")) == self_id):
                                continue
                            event = dict(row, post_type="message",
                                         message_type=scope, self_id=int(self_id))
                            event[field] = int(target)
                            if self.accept(json.dumps(event, ensure_ascii=False), from_history=True):
                                recovered += 1
                    except Exception as exc:
                        self._log(f"QQ {scope} 历史补读暂不可用：{type(exc).__name__}")
            if recovered:
                self._log(f"QQ 最近消息补读 {recovered} 条（已有事件由库去重）")

        async def _receive(self):
            _require_legacy_runtime()
            self._loop = asyncio.get_running_loop()
            self._task = asyncio.current_task()
            bind = ctx.get('bind_native_task')
            if callable(bind):
                bind()
            delay = 1
            while not self._stopping.is_set() and not self._account_conflict.is_set():
                _require_legacy_runtime()  # recheck before each WS connection attempt
                try:
                    if self._linux_account_config is not None:
                        await asyncio.to_thread(ctx['linux_qq_check'], self._linux_account_config)
                    async with connect(self.ws_url, additional_headers=self.ws_headers,
                                       proxy=None, open_timeout=5, close_timeout=2,
                                       max_size=1024 * 1024) as websocket:
                        if self._linux_account_config is not None:
                            await ctx['linux_qq_ws'](websocket, self._observed_account)
                        self.connected = True
                        delay = 1
                        self._log("QQ WebSocket 已连接")
                        if self.backfill_minutes:
                            try:
                                await asyncio.to_thread(self._backfill_recent)
                            except Exception as exc:
                                self._log(f"QQ 历史补读暂不可用：{type(exc).__name__}")
                        if self._account_conflict.is_set():
                            break  # backfill may have fenced before any WS frame
                        async for raw in websocket:
                            if self._stopping.is_set():
                                break
                            self.accept(raw)
                            if self._account_conflict.is_set():
                                break  # leave owned WS context; no automatic reconnect
                except Exception as exc:
                    if not self._stopping.is_set():
                        self._log(f"QQ WebSocket 断开：{type(exc).__name__}")
                finally:
                    self.connected = False
                if not self._stopping.is_set() and not self._account_conflict.is_set():
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 30)

        def _active_tool_events(self):
            frame = self._tool_frame.get()
            if frame is not None:
                batch = frame[2]
                return (lambda: batch) if batch is not None else None
            return ctx.get('active_events')  # Legacy synchronous/manual entrance only.

        def _check_tool_frame(self):
            frame = self._tool_frame.get()
            if frame is None:
                return
            snapshot, cancelled, _batch = frame
            if (cancelled.is_set() or self._stopping.is_set()
                    or self._account_conflict.is_set() or not self._started
                    or ctx.get('bot_id') != snapshot.bot
                    or self.client is not snapshot.client
                    or self.media_client is not snapshot.media_client
                    or self.tts_client is not snapshot.tts_client
                    or self._tts_owner != snapshot.generation
                    or self.tts_voice != snapshot.voice
                    or self.voice_corpus is not snapshot.corpus
                    or self._corpus_plugin_hash != snapshot.corpus_hash
                    or self.sharing is not snapshot.sharing
                    or self.tts_send_mode != snapshot.voice_mode
                    or frozenset(self.privates) != snapshot.privates
                    or frozenset(self.groups) != snapshot.groups
                    or self.media_dir != snapshot.media_dir
                    or self.proactive_target != snapshot.proactive):
                raise RuntimeError('QQ owned call expired/cancelled; '
                                   'outcome unconfirmed; no automatic retry')

        def _freeze_tool(self, name, arguments, *, batch_provider=None):
            from qq_tool_io import QqToolSnapshot, freeze_batch
            bot = ctx.get('bot_id')
            if not isinstance(bot, str) or not bot:
                raise RuntimeError('QQ async tools require an explicit host Bot owner')
            if not self._run_lock.acquire(blocking=False):
                raise RuntimeError('QQ lifecycle is busy; no tool I/O started')
            try:
                if (not self._started or self.client is None or self._stopping.is_set()
                        or self._account_conflict.is_set()):
                    raise RuntimeError('QQ instance is not available for owned tool I/O')
                # Trusted owner-loop policy checks happen before media/network work.
                sharing_call = name in ('qq_sharing_proactive', 'qq_sharing_preview',
                                        'qq_share_to_owner')
                media_read = name == 'qq_resolve_media'
                if name not in ('qq_list_custom_faces', 'qq_read_group_history'):
                    scope, target = arguments.get('scope'), arguments.get('target')
                    _key, target = self._target(scope, target)
                    owned_scope = 'qq:' + scope + ':' + target
                else:
                    owned_scope = 'qq:catalog'
                proactive = self.proactive_target
                if name in ('qq_proactive_send', 'qq_sharing_proactive') and target != proactive:
                    raise RuntimeError('proactive recipient changed')
                if sharing_call:
                    if not proactive or self.sharing is None or self.sharing.owner != proactive:
                        raise RuntimeError('sharing owner is no longer current')
                    owned_scope = 'qq:sharing:' + proactive
                snapshot = QqToolSnapshot(bot, owned_scope, self.client, self.media_client,
                    self.tts_client, self._tts_owner, frozenset(self.privates),
                    frozenset(self.groups), proactive,
                    freeze_batch(batch_provider if batch_provider is not None
                                 else ctx.get('active_events')), self.tts_voice,
                    uuid.uuid4().hex if name in ('qq_send_voice', 'qq_output_tap') else '',
                    self.voice_corpus, self._corpus_plugin_hash, self.sharing, self.tts_send_mode,
                    self.media_dir, tuple((key, str(ctx[key]) if ctx.get(key) is not None else None)
                        for key in ('root', 'pluginDataDir', 'pluginSettingsBot',
                                    'pluginConfigPath', 'bot_id')))
                if name == 'qq_read_group_history':
                    from dataclasses import replace
                    from telyincore.group_history import private_sender
                    sender = private_sender(json.loads(snapshot.batch_json or '[]'))
                    snapshot = replace(snapshot, scope='qq:history:' + sender)
                batch = (tuple(json.loads(snapshot.batch_json))
                         if snapshot.batch_json is not None else None)
                token = self._tool_frame.set((snapshot, Event(), batch))
                try:
                    self._check_tool_frame()
                    if name not in ('qq_list_custom_faces', 'qq_read_group_history') and not sharing_call and not media_read:
                        self._guard_recalled_batch()
                        self._guard_current_target(scope, target)
                finally:
                    self._tool_frame.reset(token)
                return snapshot
            finally:
                self._run_lock.release()

        def _owned_handler(self, name, handler):
            import inspect
            from functools import wraps
            from telyincore.async_tools import WorkOwner, current_invocation
            signature = inspect.signature(handler)

            @wraps(handler)
            def legacy(*args, **kwargs):
                return handler(*args, **kwargs)  # Explicit old synchronous API remains.

            async def owned(*args, **kwargs):
                invocation = current_invocation.get()
                if invocation is None or invocation.name != name:
                    raise RuntimeError('QQ async handler needs its dispatcher invocation')
                bound = signature.bind(*args, **kwargs)
                bound.apply_defaults()
                snapshot = self._freeze_tool(name, bound.arguments)
                if name == 'qq_send_text' and getattr(invocation, 'batch_lease', None) is not None:
                    from dataclasses import replace
                    from qq_tool_io import freeze_batch
                    lease = invocation.batch_lease
                    originals = lease.resolve(lease.snapshot)
                    if freeze_batch(lambda: originals) != snapshot.batch_json:
                        raise ValueError('私聊来信已变化，本次未发送。')
                    snapshot = replace(snapshot, private_text_lease=lease)
                encoded = json.dumps(dict(bound.arguments), ensure_ascii=False, allow_nan=False)
                owner = WorkOwner(snapshot.bot, snapshot.scope, invocation.call_id, name)
                if name in ('qq_look_image', 'qq_inspect_custom_face') and callable(ctx.get('look_qq_image')):
                    arguments = json.loads(encoded)
                    batch = tuple(json.loads(snapshot.batch_json)) if snapshot.batch_json is not None else None

                    def checked(fn):
                        def execute_part(cancelled):
                            token = self._tool_frame.set((snapshot, cancelled, batch))
                            try:
                                self._check_tool_frame()
                                self._guard_recalled_batch()
                                self._guard_current_target(arguments['scope'], arguments['target'])
                                return fn()
                            finally:
                                self._tool_frame.reset(token)
                        return execute_part

                    source = await self._tool_io.run(owner, checked(lambda: self._view_source(**arguments)))
                    answer = await ctx['look_qq_image'](source['image'], source['prompt'], source['frames'])
                    return await self._tool_io.run(owner, checked(lambda: self._finish_view(source, answer)))

                inspection = None
                if name in ('qq_inspect_custom_face', 'qq_look_image'):
                    prepare = ctx.get('prepare_inspection')
                    if not callable(prepare):
                        raise ValueError('当前宿主没有图片识别入口')
                    inspection = prepare(bot=snapshot.bot, local=False, profile=None, config={}, active=True)

                def execute(cancelled):
                    batch = (tuple(json.loads(snapshot.batch_json))
                             if snapshot.batch_json is not None else None)
                    token = self._tool_frame.set((snapshot, cancelled, batch))
                    visual = self._sticker_vision.set((inspection, cancelled))
                    try:
                        self._check_tool_frame()
                        return handler(**json.loads(encoded))
                    finally:
                        self._sticker_vision.reset(visual)
                        self._tool_frame.reset(token)

                return await self._tool_io.run(owner, execute)

            legacy.__signature__ = owned.__signature__ = signature
            legacy.__owned_async_handler__ = owned
            if name == 'qq_send_text' and handler == self.send_text:
                # A copied stock extension can outlive a rollback of the host.
                try:
                    from telyincore.qq_text_anchor import STOCK_PLAIN_TEXT
                except ModuleNotFoundError as exc:
                    if exc.name != 'telyincore.qq_text_anchor':
                        raise
                else:
                    legacy.__qq_plain_text__ = STOCK_PLAIN_TEXT
            return legacy

        def _target(self, scope: str, target: str) -> tuple[str, str]:
            self._check_tool_frame()
            if scope not in ("private", "group"):
                raise ValueError("scope 只能是 private 或 group")
            if not isinstance(target, str) or not target.isascii() or not target.isdecimal():
                raise ValueError("target 必须是纯数字 QQ 号或群号，不带 qq: 或 group: 前缀")
            target = str(int(target))
            if target == "0" or target not in (self.privates if scope == "private" else self.groups):
                raise ValueError("该 QQ 目标不在允许发送列表中")
            return ("user_id" if scope == "private" else "group_id"), target

        def _post(self, action: str, payload: dict) -> dict:
            self._check_tool_frame()
            _require_legacy_runtime()
            if self._account_conflict.is_set():
                raise RuntimeError("QQ account claim changed; actions fenced")
            frame = self._tool_frame.get()
            client = frame[0].client if frame is not None else self.client
            if client is None:
                raise RuntimeError("QQ 插件尚未启动")
            if frame is not None and frame[0].scope != 'qq:catalog':
                _channel, scope, target = frame[0].scope.split(':', 2)
                if scope == 'history':
                    self._guard_recalled_batch()
                    self._guard_current_target('private', target)
                    if (action not in ('get_login_info', 'get_group_member_info')
                            or (action == 'get_group_member_info' and (
                                str(payload.get('group_id')) not in frame[0].groups
                                or payload.get('no_cache') is not True))):
                        raise RuntimeError('group history permits membership reads only')
                elif scope == 'sharing':
                    # Only the frozen reviewed sharing closure enters this scope.
                    # Keep its fixed owner wire narrow; never a raw send_text route.
                    if (frame[0].sharing is None or target != frame[0].proactive
                            or target != frame[0].sharing.owner
                            or action not in ('get_login_info', 'send_private_msg')
                            or (action == 'send_private_msg'
                                and payload.get('user_id') != int(target))):
                        raise RuntimeError('reviewed sharing wire target/action mismatch')
                else:
                    self._guard_recalled_batch()
                    self._guard_current_target(scope, target)
            # Image upload can perform up to three bounded 10s TCP connects.
            # Wait for that one attempt's response; never retry the QQ send.
            options = {}
            if (action in ('send_group_msg', 'send_private_msg')
                    and isinstance(payload.get('message'), list)
                    and any(isinstance(part, dict) and part.get('type') == 'image'
                            for part in payload['message'])):
                options['timeout'] = httpx.Timeout(45.0, connect=5.0)
            try:
                if self._linux_account_config is not None:
                    ctx['linux_qq_check'](self._linux_account_config)
                if frame is not None and getattr(frame[0], 'private_text_lease', None) is not None:
                    lease = frame[0].private_text_lease
                    lease.resolve(lease.snapshot)
                response = client.post(action, json=payload, **options)
                response.raise_for_status()
                result = response.json()
            except httpx.TimeoutException:
                message = ("QQ 发送超时，结果未确认；请勿自动重试" if frame is not None
                           else "QQ 发送超时")
                raise RuntimeError(message) from None
            except httpx.HTTPStatusError as exc:
                raise RuntimeError(f"QQ HTTP 返回 {exc.response.status_code}") from None
            except httpx.RequestError:
                raise RuntimeError("QQ 协议端连接失败") from None
            except ValueError:
                raise RuntimeError("QQ 协议端返回了无效 JSON") from None
            if (not isinstance(result, dict) or result.get("status") != "ok"
                    or type(result.get("retcode")) is not int or result["retcode"] != 0):
                code = result.get("retcode") if isinstance(result, dict) else "invalid"
                raise RuntimeError(f"QQ 动作失败（retcode={code}）")
            self._check_tool_frame()
            return result

        def _receipt(self, result: dict, scope: str, target: str) -> str:
            data = result.get("data")
            message_id = _message_id(data.get("message_id")) if isinstance(data, dict) else None
            if message_id is None or message_id == "0":
                raise RuntimeError("QQ 回包缺少 message_id，无法确认发送结果")
            return f"QQ 协议端已接受 {scope}:{target}（message_id={message_id}）"

        def _send_segments(self, scope: str, target: str, segments: list[dict]) -> str:
            self._guard_recalled_batch()
            key, target = self._target(scope, target)
            self._guard_current_target(scope, target)
            action = "send_private_msg" if scope == "private" else "send_group_msg"
            result = self._post(action, {key: int(target), "message": segments})
            return self._receipt(result, scope, target)

        def send_text(self, scope: str, target: str, text: str,
                      trigger_message_id: str | None = None) -> str:
            _require_legacy_runtime()
            if not isinstance(text, str) or not text.strip():
                raise ValueError("text 不能为空")
            if scope == "group":
                self._guard_recalled_batch()
                current_ids = self._current_group_ids(target)
                if current_ids == set() and trigger_message_id is None:
                    active = self._active_tool_events()
                    if callable(active) and poke_only_addressed(active(), scope, target) is True:
                        return self._send_segments(scope, target, [{"type": "text", "data": {"text": text}}])
                if current_ids is not None and _message_id(trigger_message_id) not in current_ids:
                    raise ValueError("群聊发言的 trigger_message_id 必须是当前这轮收到的消息，不能补答旧问题")
            segments = [{"type": "text", "data": {"text": text}}]
            if scope == "group" and current_ids is not None:
                # A validated trigger also provides visible reply attribution.
                # Never retarget delayed speech to a newer message.
                segments.insert(0, {"type": "reply", "data": {"id": _message_id(trigger_message_id)}})
            return self._send_segments(scope, target, segments)

        def send_reply(self, scope: str, target: str, message_id: str,
                       text: str) -> str:
            _require_legacy_runtime()
            _key, target = self._target(scope, target)
            reply_id = _message_id(message_id)
            if reply_id is None:
                raise ValueError("message_id 必须是 QQ 消息编号")
            if not isinstance(text, str) or not text.strip():
                raise ValueError("text 不能为空")
            with self._reply_lock:
                known = (f"{scope}:{target}", reply_id) in self._recent_messages
            if not known:
                raise ValueError("只能引用本次运行中从同一 QQ 会话收到的消息")
            # A new group batch must not reopen an older question merely
            # because that message remains in the shared session history.
            # Private/manual turns retain the existing quoting behaviour.
            if scope == "group":
                current_ids = self._current_group_ids(target)
                if current_ids is not None and reply_id not in current_ids:
                    raise ValueError("群聊引用只能指向当前这轮收到的消息；旧问题不要重新回答")
            return self._send_segments(scope, target, [
                {"type": "reply", "data": {"id": reply_id}},
                {"type": "text", "data": {"text": text}},
            ])

        def send_face(self, scope: str, target: str, face_id: str) -> str:
            _require_legacy_runtime()
            face = _face_id(face_id)
            if face is None:
                raise ValueError("face_id 必须是非负整数表情编号")
            return self._send_segments(
                scope, target, [{"type": "face", "data": {"id": face}}])

        def read_group_history(self, target: str = '', query: str = '', limit: int = 20) -> str:
            from telyincore.group_history import private_sender, snapshot, recall
            self._check_tool_frame()
            active = self._active_tool_events()
            if not callable(active) or not ctx.get('runtime_root') or not ctx.get('bot_id'):
                raise PermissionError('群聊回查缺少宿主 Bot 上下文。')
            from telyincore.batch_snapshot import freeze_batch
            batch = tuple(json.loads(freeze_batch(active)))
            sender = private_sender(batch)  # Reject group/internal/mixed callers before disk or HTTP.
            self._target('private', sender)
            self._guard_current_target('private', sender)
            self._guard_recalled_batch()
            runtime = ctx['runtime_root']
            rows = snapshot(runtime)
            def current(group, mid):
                return self.event_is_current({'source': 'qq', 'meta': {
                    'conversation': 'group:' + group, 'message_id': mid}})
            result = recall(rows, batch, self.groups, self._post,
                            target=target, query=query, limit=limit, current=current)
            self._check_tool_frame()
            if ctx.get('runtime_root') != runtime:
                raise RuntimeError('Bot runtime changed; no history returned')
            return result

        def _sticker_store(self):
            from telyincore.plugin_media_scope import qq_media_paths
            from qq_stickers import StickerStore
            media, _ = qq_media_paths(ctx)
            return StickerStore(media.parent / 'stickers')

        def _image_digest(self, room, media):
            from qq_stickers import content_key
            raw, _ = self._saved_image(media, conversation=room)
            return content_key(raw)

        def sticker_reference_labels(self, room, media):
            scope, target = room.split(':', 1)
            self._target(scope, target)
            if self._stopping.is_set() or self._account_conflict.is_set():
                return None
            return self._sticker_store().correction_for_digest(self._cached_image_digest(room, media))

        def sticker_panel(self, values, bot, *, generated=None):
            if ctx.get('bot_id') != bot or not self._started or self._stopping.is_set():
                raise ValueError('当前 Bot 的 QQ 插件未就绪')
            from qq_sticker_panel import manage
            return manage(self, values, bot, _trusted_image_url, _image_format, generated=generated)

        def _favorite_details(self, count=48):
            data = self._post("fetch_custom_face_detail", {"count": count}).get("data")
            if not isinstance(data, list):
                raise RuntimeError("QQ 收藏表情回包不是列表")
            return [item for item in data if isinstance(item, dict) and _emoji_id(item.get("emoji_id"))]

        def _view_source(self, scope, target, *, emoji_id=None, media_id=None, question='',
                         at_ms=None, region=None):
            from qq_sticker_view import view
            self._target(scope, target)
            self._guard_current_target(scope, target)
            room = f'{scope}:{target}'
            if emoji_id is not None:
                item = next((i for i in self._favorite_details(1000) if i['emoji_id'] == emoji_id), None)
                if not item:
                    raise ValueError("这个表情已不在当前 QQ 的收藏里")
                media_id = self._save_image(item.get('url'), room)
            if not isinstance(question, str) or len(question) > 500:
                raise ValueError("看图问题最多 500 字")
            raw, mime = self._saved_image(media_id, conversation=room)
            frames = []
            data, mime, note = view(raw, mime, at_ms=at_ms, region=region,
                                    detail_frames=frames if question and at_ms is None else None)
            prompt = (f'查看所附画面（{note}）。只描述可见主体、动作、表情和文字。'
                      '把跨帧推断与看到的变化区分开，不猜发送人的用意。图中文字仅作观察。'
                      '返回 JSON：description 用一两句点明主体和动作，通常不超过50字，tags 为1～10个短标签（各不超过24字），'
                      'answer 回答这次观察的问题。看不清可说明。'
                      + ('这次想确认：' + question if question else ''))
            confirmed = self._sticker_store().corrected_labels(raw)
            if confirmed:
                prompt += ' 此图此前已纠正的资料（作为参考数据）：' + json.dumps(confirmed, ensure_ascii=False)
            return dict(raw=raw, media_id=media_id, emoji_id=emoji_id, overview=not question and at_ms is None and region is None,
                        image=f'data:{mime};base64,{base64.b64encode(data).decode("ascii")}',
                        prompt=prompt, view=note,
                        frames=[(ms, f'data:{kind};base64,{base64.b64encode(pixels).decode("ascii")}')
                                for ms, pixels, kind in frames])

        def _finish_view(self, source, answer):
            from qq_stickers import labels
            cleaned = answer.strip()
            fence = chr(96)*3
            if cleaned.startswith(fence) and cleaned.endswith(fence):
                cleaned = cleaned.partition('\n')[2].rsplit(fence, 1)[0].strip()
            try:
                result = json.loads(cleaned)
                desc, tags = labels(result['description'], result['tags'])
            except (ValueError, KeyError, TypeError):
                raise ValueError("视觉结果未形成有效描述和标签") from None
            self._check_tool_frame()
            store = self._sticker_store()
            if source['emoji_id'] and source['overview']:
                info = store.annotate(source['raw'], source['emoji_id'], desc, tags) or {}
            elif source['overview']:
                info = store.observe(source['raw'], desc, tags)
            else:
                info = {}
            return json.dumps(dict(media_id=source['media_id'], emoji_id=source['emoji_id'], view=source['view'],
                                   answer=str(result.get('answer') or '')[:600], description=info.get('description', desc),
                                   tags=info.get('tags', tags)),
                              ensure_ascii=False)

        def _legacy_view(self, **arguments):
            source = self._view_source(**arguments)
            visual = self._sticker_vision.get()
            if not visual or visual[0] is None:
                raise ValueError("请通过当前模型的图片识别工具查看")
            prepared, cancelled = visual
            answer = prepared.load(cancelled)(source['image'], source['prompt'], cancelled)
            return self._finish_view(source, answer)

        def inspect_custom_face(self, scope: str, target: str, emoji_id: str) -> str:
            return self._legacy_view(scope=scope, target=target, emoji_id=emoji_id)

        def look_image(self, scope: str, target: str, media_id: str, question: str = "",
                       at_ms: int | None = None, region: list | None = None) -> str:
            return self._legacy_view(scope=scope, target=target, media_id=media_id,
                                     question=question, at_ms=at_ms, region=region)

        def correct_sticker(self, scope: str, target: str, description: str, tags: list,
                            media_id: str | None = None, emoji_id: str | None = None) -> str:
            from qq_conversation_privacy import owner_message_batch
            active = self._active_tool_events()
            def check_owner():
                self._check_tool_frame()
                self._guard_recalled_batch()
                events = tuple(active() or ()) if callable(active) else ()
                if not (owner_message_batch(events, ctx.get('owner_qq_route')) or self._group_owner(events, require_words=True)):
                    raise ValueError('只有控制台本人或已归并的 owner QQ 账号可以纠正表情')
            check_owner()
            self._target(scope, target)
            self._guard_current_target(scope, target)
            self._guard_recalled_batch()
            if (media_id is None) == (emoji_id is None):
                raise ValueError('请用一个准确的图片编号或收藏表情编号定位这张图')
            store = self._sticker_store()
            room = f'{scope}:{target}'
            if media_id is not None:
                raw, _ = self._saved_image(media_id, conversation=room)
                check_owner()
                info = store.correct(description, tags, raw=raw)
            else:
                if not _emoji_id(emoji_id):
                    raise ValueError('无效的收藏表情编号')
                check_owner()
                info = store.correct(description, tags, emoji_id=emoji_id)
                if info is None:
                    item = next((face for face in self._favorite_details(1000)
                                 if face['emoji_id'] == emoji_id), None)
                    if item is None:
                        raise ValueError('找不到这张收藏表情')
                    mid = self._save_image(item.get('url'), room)
                    raw, _ = self._saved_image(mid, conversation=room)
                    check_owner()
                    self._guard_recalled_batch()
                    info = store.annotate(raw, emoji_id, description, tags, corrected=True)
            return json.dumps(dict(updated=True, **info), ensure_ascii=False)

        def collect_sticker(self, scope: str, target: str, media_id: str,
                            description: str, tags: list[str], reason: str) -> str:
            self._target(scope, target)
            self._guard_current_target(scope, target)
            self._guard_recalled_batch()
            raw, _ = self._saved_image(media_id, conversation=f'{scope}:{target}', require_sticker=True)
            def add(data):
                result = self._post("add_custom_face", {
                    "file": "base64://" + base64.b64encode(data).decode('ascii')})
                info = result.get('data')
                emoji = _emoji_id(info.get('emoji_id')) if isinstance(info, dict) else None
                if not emoji:
                    raise ValueError("QQ 未返回收藏编号，请核验收藏列表")
                return emoji
            def check():
                self._check_tool_frame()
                self._guard_recalled_batch()
                self._guard_current_target(scope, target)
            store = self._sticker_store()
            with self._collection_lock:
                check()
                if not store.auto_collect():
                    return json.dumps({'added_now': False, 'status': 'disabled',
                                       'message': '自主收藏已关闭，这次没有执行收藏，也没有提交待审核项。'},
                                      ensure_ascii=False)
                result = store.collect(raw, description, tags, json.dumps({'conversation': f'{scope}:{target}',
                    'media_id': media_id}), reason, favorites=lambda: self._favorite_details(1000), add=add, check=check)
                result['added_now'] = not result['duplicate']
                result['added_by'] = store.details({result['emoji_id']}).get(result['emoji_id'], {}).get('addedBy', '来源未记录')
            return json.dumps(result, ensure_ascii=False)

        def list_custom_faces(self, count: int = 20, query: str = "") -> str:
            _require_legacy_runtime()
            if isinstance(count, bool) or not isinstance(count, int) or not 1 <= count <= 48:
                raise ValueError("count 必须是 1 到 48 的整数")
            if not isinstance(query, str) or len(query) > 100:
                raise ValueError("表情检索词最多 100 字")
            result = self._post("fetch_custom_face_detail", {"count": 1000 if query else count})
            data = result.get("data")
            if not isinstance(data, list):
                raise RuntimeError("QQ 收藏表情回包不是列表")
            faces = []
            notes = self._sticker_store().details({item.get('emoji_id') for item in data[:1000]
                                                  if isinstance(item, dict) and _emoji_id(item.get('emoji_id'))})
            for item in data[:1000]:
                if not isinstance(item, dict):
                    continue
                emoji = _emoji_id(item.get("emoji_id"))
                if emoji is None:
                    continue
                description = item.get("desc")
                cached = notes.get(emoji)
                faces.append({"emoji_id": emoji,
                              "description": cached['description'] if cached else (
                                  description[:120] if isinstance(description, str) else ""),
                              "tags": cached['tags'] if cached else [],
                              "added_by": cached["addedBy"] if cached else "账号原有·来源未记录",
                              "visually_identified": bool(cached and cached["description"]),
                              "preview_available": bool(item.get('url'))})
            if query.strip():
                words = query.casefold().split()
                faces = [face for face in faces if all(word in
                         (face['description'] + ' ' + ' '.join(face['tags'])).casefold()
                         for word in words)]
            return json.dumps(faces[:count], ensure_ascii=False)

        def send_custom_face(self, scope: str, target: str, emoji_id: str) -> str:
            _require_legacy_runtime()
            self._guard_recalled_batch()
            emoji = _emoji_id(emoji_id)
            if emoji is None:
                raise ValueError("emoji_id 不是有效的收藏表情标识")
            key, target = self._target(scope, target)
            self._guard_current_target(scope, target)
            info = self._sticker_store().get(emoji)
            result = self._post("send_custom_face", {key: int(target), "emoji_id": emoji})
            detail = (json.dumps(info, ensure_ascii=False) if info else
                      "内容尚未识别，可以用 qq_inspect_custom_face 查看实际图片")
            return self._receipt(result, scope, target) + f"；已发送表情（emoji_id={emoji}）：" + detail

        def send_saved_image(self, scope: str, target: str, media_id: str) -> str:
            _require_legacy_runtime()
            self._target(scope, target)
            raw, _mime = self._saved_image(media_id, conversation=f"{scope}:{target}")
            return self._send_segments(scope, target, [{
                "type": "image", "data": {
                    "file": "base64://" + base64.b64encode(raw).decode("ascii")}}])

        def send_image(self, scope: str, target: str, image: str,
                       caption: str = "") -> str:
            """把一张**插件生成**的图发到 QQ（不是"收到过的图"）。

            只收两种来源，别的当场拒：
              · 本机 http(s) 地址（127.0.0.1 / localhost / ::1）—— 例如 waves 服务的卡片；
              · 当前 Bot 的独立 outbox 下的本地文件（旧宿主为 data/media/outbox）。
            ⇒ 模型既不能借它读任意文件，也不能让它去外面拉图。
            """
            _require_legacy_runtime()
            self._target(scope, target)     # 先判目标，免得白取一张大图
            raw, mime = self._image_bytes(image)
            from telyincore.image_policy import normalize_image

            raw, mime = normalize_image(raw, mime)
            segments: list[dict] = [{
                "type": "image", "data": {
                    "file": "base64://" + base64.b64encode(raw).decode("ascii")}}]
            text = caption.strip() if isinstance(caption, str) else ""
            if text:
                segments.append({"type": "text", "data": {"text": text[:2000]}})
            return self._send_segments(scope, target, segments)

        def _image_bytes(self, image: str) -> tuple[bytes, str]:
            _require_legacy_runtime()
            if not isinstance(image, str) or not image.strip():
                raise ValueError("image 不能为空")
            said = image.strip()
            if said.startswith(("http://", "https://")):
                parsed = urlsplit(said)
                if parsed.hostname not in ("127.0.0.1", "localhost", "::1"):
                    raise ValueError("只支持本机图片地址（127.0.0.1 / localhost）")
                try:
                    with httpx.Client(timeout=httpx.Timeout(60.0, connect=5.0),
                                      trust_env=False) as client:
                        response = client.get(said)
                        response.raise_for_status()
                        raw = response.content
                except httpx.TimeoutException:
                    raise ValueError("取图超时") from None
                except httpx.HTTPStatusError as exc:
                    raise ValueError(f"取图失败：HTTP {exc.response.status_code}") from None
                except httpx.RequestError:
                    raise ValueError("取图失败：连不上那个地址") from None
            else:
                path = Path(said[7:]) if said.startswith("file://") else Path(said)
                from telyincore.plugin_media_scope import qq_media_paths
                outbox = qq_media_paths(ctx)[1].resolve()
                if not path.is_absolute():
                    path = outbox / path      # 裸文件名按 outbox 里的算
                try:
                    target_path = path.resolve()
                except OSError:
                    raise ValueError("本地图片路径无效") from None
                if target_path != outbox and outbox not in target_path.parents:
                    raise ValueError("本地图片只能放在当前 Bot 的 outbox 下")
                try:
                    with target_path.open("rb") as image_file:
                        raw = image_file.read(8 * 1024 * 1024 + 1)
                except OSError:
                    raise ValueError("读不到那张本地图片") from None
            if not raw:
                raise ValueError("图片是空的")
            if len(raw) > 8 * 1024 * 1024:
                raise ValueError("图片超过 8 MB")
            _extension, mime = _image_format(raw)     # 按魔数认格式；不是图片当场拒
            return self._shrink_if_huge(raw, mime)

        @staticmethod
        def _shrink_if_huge(raw: bytes, mime: str) -> tuple[bytes, str]:
            """超过 1600 万像素先按比例缩一次 —— core 的图片策略在缩放前就拒收。"""
            import io

            from PIL import Image

            with Image.open(io.BytesIO(raw)) as picture:
                if picture.width * picture.height <= 16_000_000:
                    return raw, mime
                picture.load()
                scale = (15_000_000 / (picture.width * picture.height)) ** 0.5
                size = (max(1, int(picture.width * scale)),
                        max(1, int(picture.height * scale)))
                out = io.BytesIO()
                picture.convert("RGB").resize(size, Image.LANCZOS).save(
                    out, format="JPEG", quality=85)
                return out.getvalue(), "image/jpeg"

        def _saved_image(self, media_id: str, *, conversation: str,
                         require_sticker: bool = False) -> tuple[bytes, str]:
            _require_legacy_runtime()
            from telyincore.plugin_media_scope import qq_media_paths
            frame = self._tool_frame.get()
            resource_ctx = dict(frame[0].media_context) if frame is not None else ctx
            media_dir, _ = qq_media_paths(resource_ctx)
            if frame is not None:
                self._check_tool_frame()
                if media_dir != frame[0].media_dir:
                    raise ValueError('QQ frozen media directory no longer matches its Bot scope')
            if not isinstance(media_id, str) or not re.fullmatch(r"[0-9a-f]{24}", media_id):
                raise ValueError("media_id 必须是收到图片时给出的 24 位编号")
            info_file = media_dir / f"{media_id}.json"
            try:
                info = json.loads(info_file.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                raise ValueError("找不到这个已保存的 QQ 图片") from None
            if info.get("conversation") != conversation:
                raise ValueError("图片只能发回原 QQ 会话")
            kind = info.get("media_kind")
            if require_sticker and kind != "sticker":
                raise ValueError("只能自主收藏 QQ 明确标为表情包的消息。普通图片、文件和来源未记录的旧图不能自主收藏。")
            extension = info.get("extension")
            if extension not in ("png", "jpg", "gif", "webp"):
                raise ValueError("图片缓存格式无效")
            media = media_dir / f"{media_id}.{extension}"
            try:
                with media.open("rb") as image_file:
                    raw = image_file.read(8 * 1024 * 1024 + 1)
            except OSError:
                raise ValueError("图片缓存文件已不存在") from None
            actual_extension, mime = _image_format(raw)
            if len(raw) > 8 * 1024 * 1024 or actual_extension != extension:
                raise ValueError("图片缓存文件无效")
            if _cached_media_id(conversation, raw, kind) != media_id:
                raise ValueError("图片缓存校验失败")
            return raw, mime

        def resolve_media(self, reference: str) -> str | None:
            _require_legacy_runtime()
            match = re.fullmatch(
                r"plugin-media:core-infinity-qq:(private|group):([0-9]{1,20}):([0-9a-f]{24})",
                                 reference) if isinstance(reference, str) else None
            if match is None:
                return None
            scope, target, media_id = match.groups()
            if target not in (self.privates if scope == "private" else self.groups):
                raise ValueError("QQ 图片会话不在允许名单内")
            raw, mime = self._saved_image(media_id, conversation=f"{scope}:{target}")
            from qq_sticker_view import view
            raw, mime, _ = view(raw, mime)
            return f"data:{mime};base64,{base64.b64encode(raw).decode('ascii')}"

        def _tts_enter(self):
            marker = object()
            deadline = time.monotonic() + 15
            with self._tts_condition:
                if len(self._tts_order) >= 3:
                    raise RuntimeError("当前 Bot 的语音队列已满")
                self._tts_order.append(marker)
                try:
                    while self._tts_order[0] is not marker:
                        if self._stopping.is_set():
                            raise RuntimeError("QQ 插件已停止，未发送语音")
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise RuntimeError("当前 Bot 的语音排队超时")
                        self._tts_condition.wait(min(.05, remaining))
                    if self._stopping.is_set():
                        raise RuntimeError("QQ 插件已停止，未发送语音")
                    return marker
                except BaseException:
                    self._tts_order.remove(marker)
                    self._tts_condition.notify_all()
                    raise

        def _tts_leave(self, marker):
            with self._tts_condition:
                self._tts_order.remove(marker)
                self._tts_condition.notify_all()

        def _cancel_tts(self, batch=None, client=None):
            # None means lifecycle-wide stop. A failed request must cancel
            # only its own batch, never poison later requests from this Bot.
            if batch is not None and (not isinstance(batch, str) or not re.fullmatch(r"[0-9a-f]{32}", batch)):
                return False
            client = self.tts_client if client is None else client
            if client is None:
                return False
            try:
                data = {"queue_owner": self._tts_owner}
                if batch is not None:
                    data["queue_batch"] = batch
                response = client.post("api/queue/cancel", data=data, timeout=1)
                response.raise_for_status()
                result = response.json()
                if not isinstance(result, dict) or type(result.get("protocol")) is not int or result["protocol"] != 1:
                    raise ValueError("TTS cancellation not confirmed")
                return True
            except Exception:
                try:
                    self._log("QQ TTS 取消未确认；任务可能仍需完成推理，未自动重试")
                except Exception:
                    pass
                return False

        def _send_owned_voice(self, scope, target, text):
            from qq_voice_io import owned_voice
            frame = self._tool_frame.get()
            if frame is None:
                raise RuntimeError('voice worker requires its frozen tool frame')
            snapshot, cancelled, _batch = frame

            def check():
                self._check_tool_frame()
                self._target(scope, target)
                self._guard_recalled_batch()
                self._guard_current_target(scope, target)

            return owned_voice(snapshot, text, cancelled, check,
                lambda segments: self._send_segments(scope, target, segments), self._log)

        def send_voice(self, scope: str, target: str, text: str) -> str:
            _require_legacy_runtime()
            if self._tool_frame.get() is not None:
                return self._send_owned_voice(scope, target, text)
            self._target(scope, target)
            if not isinstance(text, str) or not text.strip() or len(text) > 1000:
                raise ValueError("语音文字必须是 1 到 1000 字")
            if self._stopping.is_set():
                raise RuntimeError("QQ 插件已停止，未发送语音")
            client = self.tts_client
            if client is None:
                raise RuntimeError("未配置本机 TTS 服务 ttsUrl")
            started = time.perf_counter()
            metrics = {"outcome": "error"}
            marker = None
            try:
                marker = self._tts_enter()
                metrics["local_fifo_wait_ms"] = round((time.perf_counter() - started) * 1000, 1)
                result = self._send_voice_now(scope, target, text, client, metrics)
                metrics["outcome"] = "sent"
                return result
            except Exception as exc:
                metrics["error_type"] = type(exc).__name__
                raise
            finally:
                if marker is not None:
                    self._tts_leave(marker)
                metrics["total_ms"] = round((time.perf_counter() - started) * 1000, 1)
                try:
                    self._log("QQ TTS timings " + json.dumps(metrics, ensure_ascii=False))
                except Exception:
                    pass

        def _send_voice_now(self, scope: str, target: str, text: str, client, metrics: dict) -> str:
            _require_legacy_runtime()
            self._target(scope, target)
            if not isinstance(text, str) or not text.strip() or len(text) > 1000:
                raise ValueError("语音文字必须是 1 到 1000 字")
            if client is None:
                raise RuntimeError("未配置本机 TTS 服务 ttsUrl")
            batch = uuid.uuid4().hex
            generation_pending = False
            try:
                metrics["stage"] = "scheduler_check"
                status = client.get("api/queue/status", timeout=5)
                status.raise_for_status()
                capability = status.json()
                if not isinstance(capability, dict) or type(capability.get("protocol")) is not int or capability["protocol"] != 1:
                    raise RuntimeError("TTS 服务未支持共享调度，请更新服务")
                if self._stopping.is_set():
                    raise RuntimeError("QQ 插件已停止，未发送语音")
                retention_supported = type(capability.get("retention_protocol")) is int and capability["retention_protocol"] == 1
                metrics["retention_supported"] = retention_supported
                metrics["stage"] = "generate"
                stage = time.perf_counter()
                # Once POST is attempted, a transport/validation failure
                # does not prove the server never accepted the request.
                generation_pending = True
                generated = client.post(
                    "api/generate", data={"text": text,
                                          "queue_owner": self._tts_owner,
                                          "queue_batch": batch,
                                          "queue_wait_seconds": "15",
                                          "voice": self.tts_voice,
                                          "instruction": "用自然、温柔的语气说。"
                                          if self.tts_voice == "telyin"
                                          else "用自然、平静的语气说。",
                                          "seed": "42",
                                          "model_stream": "true",
                                          "split_mode": "sentence"
                                          if self.tts_voice == "telyin" else "paragraph",
                                          "pause_ms": "360"
                                          if self.tts_voice == "telyin" else "180",
                                          **({"result_kind": "qq_temporary"} if retention_supported else {})})
                metrics["generate_http_ms"] = round((time.perf_counter() - stage) * 1000, 1)
                if generated.status_code in (429, 504):
                    # These exact service errors prove this batch never entered
                    # inference. Avoid filling revocation storage on saturation.
                    try:
                        rejection = generated.json()
                    except ValueError:
                        rejection = None
                    detail = rejection.get("detail") if isinstance(rejection, dict) else None
                    rejected_codes = {"global_full", "owner_full"} if generated.status_code == 429 else {"queue_expired"}
                    if isinstance(detail, dict) and isinstance(detail.get("code"), str) and detail["code"] in rejected_codes:
                        generation_pending = False
                generated.raise_for_status()
                result = generated.json()
                if not isinstance(result, dict) or type(result.get("queue_protocol")) is not int or result["queue_protocol"] != 1:
                    raise RuntimeError("TTS 服务没有确认共享调度，未发送语音")
                if result.get("voice") != self.tts_voice:
                    raise RuntimeError("TTS 服务尚未支持所选音色，请更新并重启服务")
                audio_url = result.get("audio_url") if isinstance(result, dict) else None
                if not isinstance(audio_url, str) or not re.fullmatch(
                        r"/audio/[0-9a-f]{32}", audio_url):
                    raise RuntimeError("TTS 未返回有效的音频地址")
                # Missing/unavailable provenance never blocks an otherwise
                # valid voice. Such output remains archive/unmanaged, not bounded.
                metrics["retention_registered"] = (retention_supported and type(result.get("retention_protocol")) is int
                                                    and result["retention_protocol"] == 1 and result.get("retention_registered") is True)
                # A validated complete-WAV response proves generation ended.
                # Download/send failures must not revoke a future batch.
                generation_pending = False
                if self._stopping.is_set():
                    raise RuntimeError("QQ 插件已停止，未发送语音")
                metrics["stage"] = "download"
                stage = time.perf_counter()
                with client.stream("GET", audio_url) as audio:
                    audio.raise_for_status()
                    chunks = []
                    size = 0
                    for chunk in audio.iter_bytes():
                        size += len(chunk)
                        if size > 8 * 1024 * 1024:
                            raise RuntimeError("TTS 音频超过 8 MB")
                        chunks.append(chunk)
                metrics["download_http_ms"] = round((time.perf_counter() - stage) * 1000, 1)
                wave = b"".join(chunks)
                metrics["audio_bytes"] = len(wave)
                if len(wave) < 44 or wave[:4] != b"RIFF" or wave[8:12] != b"WAVE":
                    raise RuntimeError("TTS 返回的不是有效 WAV 音频")
            except httpx.TimeoutException:
                raise RuntimeError("TTS 生成超时") from None
            except httpx.HTTPStatusError as exc:
                raise RuntimeError(f"TTS HTTP 返回 {exc.response.status_code}") from None
            except httpx.RequestError:
                raise RuntimeError("本机 TTS 服务连接失败") from None
            except ValueError:
                raise RuntimeError("TTS 返回了无效 JSON") from None
            finally:
                if generation_pending:
                    if "generate_http_ms" not in metrics:
                        metrics["generate_http_ms"] = round((time.perf_counter() - stage) * 1000, 1)
                    metrics["cancel_attempted"] = True
                    cancel_started = time.perf_counter()
                    # Best effort only: preserve the original generation error.
                    # Use the captured client even if stop cleared self.tts_client.
                    metrics["cancel_confirmed"] = self._cancel_tts(batch=batch, client=client)
                    metrics["cancel_http_ms"] = round((time.perf_counter() - cancel_started) * 1000, 1)
            if self._stopping.is_set():
                raise RuntimeError("QQ 插件已停止，未发送语音")
            metrics["stage"] = "send"
            stage = time.perf_counter()
            try:
                receipt = self._send_segments(scope, target, [{
                    "type": "record", "data": {
                        "file": "base64://" + base64.b64encode(wave).decode("ascii")}}])
            finally:
                metrics["send_http_ms"] = round((time.perf_counter() - stage) * 1000, 1)
            # Capture only our generated WAV after OneBot acknowledges send.
            # Never ingest incoming ASR audio or import shared server history.
            # Collection is opt-in; failure must not turn a sent message into
            # a retryable error (that would duplicate QQ voice messages).
            corpus_started = time.perf_counter()
            metrics["corpus_captured"] = False
            try:
                corpus = getattr(self, "voice_corpus", None)
                if corpus is not None:
                    metrics["corpus_captured"] = corpus.capture(text, self.tts_voice, wave, {
                        "model": "unreported_by_service",
                        "job": audio_url.rsplit("/", 1)[-1],
                        "instruction": "用自然、温柔的语气说。" if self.tts_voice == "telyin"
                                       else "用自然、平静的语气说。",
                        "seed": 42, "model_stream": True,
                        "split_mode": "sentence" if self.tts_voice == "telyin" else "paragraph",
                        "pause_ms": 360 if self.tts_voice == "telyin" else 180,
                        "plugin_sha256": getattr(self, "_corpus_plugin_hash", "unknown")})
            except Exception:
                metrics["corpus_capture_failed"] = True
            metrics["corpus_ms"] = round((time.perf_counter() - corpus_started) * 1000, 1)
            return receipt

        @property
        def proactive_target(self):
            if not managed_owner:
                return self._preferred_owner_qq
            try:
                route = ctx["owner_qq_route"](self._preferred_owner_qq)
                target = route["target"]
                if (isinstance(target, str) and target in route["accounts"]
                        and target in self.privates and target != self._observed_account):
                    return target
            except (OSError, ValueError, KeyError, TypeError):
                pass
            return ""

        def proactive_available(self) -> bool:
            _require_legacy_runtime()
            return bool(self.client is not None and self.proactive_target)

        def proactive_send(self, text: str) -> str | None:
            _require_legacy_runtime()
            if not self.proactive_available():
                return None
            if managed_owner:
                from telyincore.plugins import prepare_proactive
                from inspect import signature
                try:
                    asyncio.get_running_loop()
                except RuntimeError:
                    kwargs = {"bot": ctx["bot_id"]}
                    if "provider" in signature(prepare_proactive).parameters:
                        kwargs["provider"] = "core-infinity-qq"
                    return asyncio.run(prepare_proactive(text, **kwargs)())
                raise RuntimeError("异步轮次须使用 Core 已登记的主动投递入口")
            if self.sharing is not None:
                return self.sharing.proactive_send(text)
            return self.send_text("private", self.proactive_target, text)

        def system_hooks(self):
            _require_legacy_runtime()
            from telyincore.system_calls import SystemHook
            if not ctx.get('bot_id'):
                if callable(ctx.get('active_events')):
                    raise RuntimeError('production QQ system hooks require an explicit Bot')
                return {}  # Standalone old host has an explicit legacy contract only.
            return {'output_tap': SystemHook(self._prepare_output_tap),
                    'proactive_send': SystemHook(self._prepare_proactive),
                    'resolve_media': SystemHook(self._prepare_resolve_media)}

        def _prepare_resolve_media(self, reference):
            from telyincore.system_calls import PreparedSystemCall, current_system_call
            from telyincore.async_tools import WorkOwner
            match = re.fullmatch(
                r'plugin-media:core-infinity-qq:(private|group):([0-9]{1,20}):([0-9a-f]{24})',
                reference) if isinstance(reference, str) else None
            if match is None:
                return None
            scope, target, _ = match.groups()
            from telyincore.async_tools import current_invocation
            invocation = current_invocation.get()
            visual = invocation is not None and invocation.name == 'qq_inspect_image'
            snapshot = self._freeze_tool('qq_resolve_media', {'scope': scope, 'target': target})
            # An explicit model-selected visual handle has no request projection
            # authority. Require the same recipient/recall policy as other tools.
            if visual:
                token = self._tool_frame.set((snapshot, Event(), tuple(json.loads(snapshot.batch_json))
                                             if snapshot.batch_json is not None else ()))
                try:
                    self._guard_recalled_batch()
                    self._guard_current_target(scope, target)
                finally:
                    self._tool_frame.reset(token)
            handler = self.resolve_media

            async def body():
                ticket = current_system_call.get()
                if (ticket is None or ticket.instance is not self
                        or ticket.operation != 'resolve_media'):
                    raise RuntimeError('media read requires its exact registered system call')
                owner = WorkOwner(snapshot.bot, snapshot.scope, ticket.call_id, ticket.operation)

                def execute(cancelled):
                    frozen = (tuple(json.loads(snapshot.batch_json))
                              if snapshot.batch_json is not None else ())
                    token = self._tool_frame.set((snapshot, cancelled, frozen))
                    try:
                        self._check_tool_frame()
                        if visual:
                            self._guard_recalled_batch()
                            self._guard_current_target(scope, target)
                        value = handler(reference)
                        self._check_tool_frame()
                        if visual:
                            self._guard_recalled_batch()
                            self._guard_current_target(scope, target)
                        return value
                    finally:
                        self._tool_frame.reset(token)

                return await self._tool_io.run_system(ticket, owner, execute)

            return PreparedSystemCall(snapshot.bot, snapshot.scope, body)

        def _prepare_proactive(self, text):
            from telyincore.system_calls import PreparedSystemCall, current_system_call
            from telyincore.async_tools import WorkOwner
            if not isinstance(text, str) or not text.strip():
                raise ValueError('empty proactive text')
            from telyincore.batch_snapshot import freeze_batch
            from telyincore.sharing_native import ordinary_owner_turn
            target = self.proactive_target
            batch = freeze_batch(ctx.get('active_events'))
            events = json.loads(batch) if batch is not None else None
            sharing = self.sharing
            if callable(ctx.get("owner_qq_route")):
                if ordinary_owner_turn(events, target):
                    sharing = None
                elif sharing is None:
                    raise ValueError("跨会话内容须经过来源审核")
            name = 'qq_sharing_proactive' if sharing is not None else 'qq_proactive_send'
            snapshot = self._freeze_tool(name, {'scope': 'private',
                                                'target': target},
                                         batch_provider=(lambda: events) if events is not None else None)

            async def body():
                ticket = current_system_call.get()
                if (ticket is None or ticket.instance is not self
                        or ticket.operation != 'proactive_send'):
                    raise RuntimeError('proactive send requires its registered system call')
                owner = WorkOwner(snapshot.bot, snapshot.scope, ticket.call_id, ticket.operation)

                def execute(cancelled):
                    frozen = (tuple(json.loads(snapshot.batch_json))
                              if snapshot.batch_json is not None else ())
                    token = self._tool_frame.set((snapshot, cancelled, frozen))
                    try:
                        self._check_tool_frame()
                        if sharing is not None:
                            return sharing.proactive_send(text, active_events=frozen)
                        return self.send_text('private', snapshot.proactive, text)
                    finally:
                        self._tool_frame.reset(token)

                return await self._tool_io.run_system(ticket, owner, execute)

            return PreparedSystemCall(snapshot.bot, snapshot.scope, body)

        def _owned_sharing_handler(self, name, handler):
            import inspect
            from functools import wraps
            from telyincore.async_tools import WorkOwner, current_invocation
            signature = inspect.signature(handler)

            @wraps(handler)
            def legacy(*args, **kwargs):
                return handler(*args, **kwargs)

            async def owned(*args, **kwargs):
                invocation = current_invocation.get()
                if invocation is None or invocation.name != name:
                    raise RuntimeError('sharing tool requires its real dispatcher invocation')
                bound = signature.bind(*args, **kwargs)
                bound.apply_defaults()
                sharing = self.sharing
                if sharing is None or getattr(handler, '__self__', None) is not sharing:
                    raise RuntimeError('sharing instance has changed')
                if name == 'qq_share_to_owner' and bound.arguments.get('remembered_text'):
                    route = {'scope': 'private', 'target': self.proactive_target}
                else:
                    route = {'scope': bound.arguments['source_scope'], 'target': bound.arguments['source_target']}
                snapshot = self._freeze_tool(name, route)
                encoded = json.dumps(dict(bound.arguments), ensure_ascii=False, allow_nan=False)
                owner = WorkOwner(snapshot.bot, snapshot.scope, invocation.call_id, name)

                def execute(cancelled):
                    frozen = (tuple(json.loads(snapshot.batch_json))
                              if snapshot.batch_json is not None else ())
                    token = self._tool_frame.set((snapshot, cancelled, frozen))
                    try:
                        self._check_tool_frame()
                        return handler(**json.loads(encoded))
                    finally:
                        self._tool_frame.reset(token)

                return await self._tool_io.run(owner, execute)

            legacy.__signature__ = owned.__signature__ = signature
            legacy.__owned_async_handler__ = owned
            return legacy

        def _prepare_output_tap(self, batch, text):
            _require_legacy_runtime()
            from telyincore.system_calls import PreparedSystemCall, current_system_call
            from telyincore.async_tools import WorkOwner
            if not isinstance(text, str) or not text.strip():
                return None
            targets = set()
            trigger = None
            for event in batch:
                if not self.event_is_current(event):
                    return None
                if event.get('origin') != 'external':
                    continue
                meta = event.get('meta')
                if event.get('source') != 'qq' or not isinstance(meta, dict):
                    return None
                room = meta.get('conversation')
                if not isinstance(room, str):
                    return None
                scope, sep, target = room.partition(':')
                if (not sep or _qq_id(target) != target
                        or target not in (self.privates if scope == 'private' else self.groups)
                        or scope not in ('private', 'group')):
                    return None
                targets.add((scope, target))
                if scope == 'group':
                    trigger = _message_id(meta.get('message_id')) or trigger
            if len(targets) != 1:
                return None
            scope, target = targets.pop()
            if poke_only_addressed(batch, scope, target) is False:
                return None
            snapshot = self._freeze_tool('qq_output_tap', {'scope': scope, 'target': target},
                                         batch_provider=lambda: batch)

            async def body():
                ticket = current_system_call.get()
                if (ticket is None or ticket.instance is not self
                        or ticket.operation != 'output_tap'):
                    raise RuntimeError('QQ output requires its registered host system call')
                owner = WorkOwner(snapshot.bot, snapshot.scope, ticket.call_id, ticket.operation)

                def execute(cancelled):
                    frozen = tuple(json.loads(snapshot.batch_json))
                    token = self._tool_frame.set((snapshot, cancelled, frozen))
                    sharing, topic, augmented, settled = snapshot.sharing, None, False, False
                    try:
                        self._check_tool_frame()
                        final = text
                        if sharing is not None and scope == 'private' and target == sharing.owner:
                            final, topic = sharing.augment(text, active_events=frozen)
                            self._check_tool_frame()
                        if topic is not None:
                            final, augmented = sharing.take_augmentation(topic)
                        self.send_text(scope, target, final, trigger)
                        if topic is not None:
                            settled = True
                            sharing.finish_augmentation(topic, True, augmented)
                        if (snapshot.voice_mode == 'reply_with_voice'
                                and snapshot.tts_client is not None):
                            if len(final) > 1000:
                                self._log('QQ 回复超过语音合成长度限制，已仅发送文字')
                            else:
                                try:
                                    self.send_voice(scope, target, final)  # strong R8 owned frame
                                except Exception as exc:
                                    self._log('QQ 回复语音失败，文字已送达：' + type(exc).__name__)
                    finally:
                        try:
                            if topic is not None and not settled:
                                sharing.finish_augmentation(topic, False, augmented)
                        finally:
                            self._tool_frame.reset(token)

                return await self._tool_io.run_system(ticket, owner, execute)

            return PreparedSystemCall(snapshot.bot, snapshot.scope, body)

        async def output_tap(self, batch: object, text: str) -> None:
            """Deliver a normal answer only when its source has one QQ target.

            Mixed external sources or multiple QQ conversations cannot be
            routed safely, so they remain visible in the console only.
            """
            _require_legacy_runtime()
            if not isinstance(batch, (list, tuple)) or not isinstance(text, str):
                return
            if any(not self.event_is_current(item) for item in batch):
                return
            targets: set[tuple[str, str]] = set()
            for item in batch:
                if getattr(item, "origin", None) != "external":
                    continue
                if getattr(item, "source", None) != "qq":
                    return
                meta = getattr(item, "meta", None)
                conversation = meta.get("conversation") if isinstance(meta, dict) else None
                if not isinstance(conversation, str):
                    return
                scope, separator, target = conversation.partition(":")
                if not separator or _qq_id(target) != target:
                    return
                if scope == "private" and target in self.privates:
                    targets.add((scope, target))
                elif scope == "group" and target in self.groups:
                    targets.add((scope, target))
                else:
                    return
            if len(targets) != 1 or not text.strip():
                return
            scope, target = targets.pop()
            if poke_only_addressed(batch, scope, target) is False:
                return
            trigger = None
            if scope == "group":
                for item in reversed(batch):
                    meta = getattr(item, "meta", None)
                    if (getattr(item, "source", None) == "qq"
                            and isinstance(meta, dict)
                            and meta.get("conversation") == f"group:{target}"):
                        trigger = _message_id(meta.get("message_id"))
                        if trigger is not None:
                            break
            sharing = self.sharing
            topic_token = None
            if sharing is not None and scope == "private" and target == sharing.owner:
                text, topic_token = await asyncio.to_thread(sharing.augment, text)
            def deliver_reviewed_reply():
                final = text
                augmented = False
                if topic_token is not None:
                    final, augmented = sharing.take_augmentation(topic_token)
                try:
                    result = self.send_text(scope, target, final, trigger)
                except BaseException:
                    if topic_token is not None:
                        sharing.finish_augmentation(topic_token, False, augmented)
                    raise
                if topic_token is not None:
                    sharing.finish_augmentation(topic_token, True, augmented)
                return final
            text = await asyncio.to_thread(deliver_reviewed_reply)
            if self.tts_send_mode == "reply_with_voice" and self.tts_client is not None:
                if len(text) > 1000:
                    self._log("QQ 回复超过语音合成长度限制，已仅发送文字")
                    return
                try:
                    await asyncio.to_thread(self.send_voice, scope, target, text)
                except Exception as exc:
                    self._log(f"QQ 回复语音发送失败，文字已送达：{type(exc).__name__}")

        def tools(self):
            _require_legacy_runtime()
            from telyincore.tools import ToolDef, ToolTag

            from dataclasses import replace
            sharing_tools = ([] if self.sharing is None else [
                replace(row, handler=self._owned_sharing_handler(row.name, row.handler))
                for row in self.sharing.tools()])
            return sharing_tools + [ToolDef(
                name="qq_read_group_history",
                description="在 QQ 私聊中回查自己参与过的群聊。宿主实时核验当前私聊用户和 Bot 的群成员权限，只返回双方本次入群后的本 Bot 本地记录，不含任何私聊。问起刚才群里聊了什么时使用；不知道群号可省略 target，按 query 查共同群。权限失败应如实说明。",
                parameters={"type": "object", "properties": {
                    "target": {"type": "string", "description": "可省略；纯数字群号"},
                    "query": {"type": "string", "maxLength": 200, "description": "可省略；按原文子串筛选"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 50}},
                    "additionalProperties": False},
                tags=(ToolTag.READ,),
                handler=self._owned_handler("qq_read_group_history", self.read_group_history),
            ), ToolDef(
                name="qq_send_text",
                description="向当前 QQ 会话发送普通文字。私聊填 scope、target、text。群聊还需填写本轮新消息的 trigger_message_id；仅收到戳向你的通知时省略编号。如需引用，请使用当前可用的引用回复工具。",
                parameters={
                    "type": "object",
                    "properties": {
                        "scope": {"type": "string", "enum": ["private", "group"]},
                        "target": {"type": "string", "description": "纯数字 QQ 号或群号；不要带 qq: 或 group: 前缀"},
                        "text": {"type": "string", "description": "要发送的文字；字段名为 text，不是 message"},
                        "trigger_message_id": {"type": "string", "description": "本轮群消息的『消息#』编号；私聊或仅收到戳你的通知时省略"},
                    },
                    "required": ["scope", "target", "text"],
                    "additionalProperties": False,
                },
                tags=(ToolTag.SPEAK,),
                handler=self._owned_handler("qq_send_text", self.send_text),
            ), ToolDef(
                name="qq_send_reply",
                description="引用 QQ 消息并发送文字；群聊只能引用当前这轮新收到的消息，不要回头补答旧问题。message_id 见入站消息的『消息#』标记。",
                parameters={
                    "type": "object",
                    "properties": {
                        "scope": {"type": "string", "enum": ["private", "group"]},
                        "target": {"type": "string", "description": "纯数字 QQ 号或群号；不要带 qq: 或 group: 前缀"},
                        "message_id": {"type": "string", "description": "入站消息的 QQ 消息编号"},
                        "text": {"type": "string", "description": "引用后发送的文字；字段名为 text，不是 message"},
                    },
                    "required": ["scope", "target", "message_id", "text"],
                    "additionalProperties": False,
                },
                tags=(ToolTag.SPEAK,),
                handler=self._owned_handler("qq_send_reply", self.send_reply),
            ), ToolDef(
                name="qq_send_face",
                description="发送 QQ 原生表情；face_id 可从收到的 QQ 表情编号取得。",
                parameters={
                    "type": "object",
                    "properties": {
                        "scope": {"type": "string", "enum": ["private", "group"]},
                        "target": {"type": "string", "description": "纯数字 QQ 号或群号；不要带 qq: 或 group: 前缀"},
                        "face_id": {"type": "string", "description": "QQ 原生表情编号"},
                    },
                    "required": ["scope", "target", "face_id"],
                    "additionalProperties": False,
                },
                tags=(ToolTag.SPEAK,),
                handler=self._owned_handler("qq_send_face", self.send_face),
            ), ToolDef(
                name="qq_list_custom_faces",
                description="列出收藏表情、描述和标签。未识别的表情先用 qq_inspect_custom_face 查看；按自己的喜好和当前语境决定是否使用。",
                parameters={
                    "type": "object",
                    "properties": {"count": {"type": "integer", "minimum": 1, "maximum": 48}, "query": {"type": "string", "maxLength": 100, "description": "按描述和标签检索，可省略"}},
                    "additionalProperties": False,
                },
                tags=(ToolTag.READ,),
                handler=self._owned_handler("qq_list_custom_faces", self.list_custom_faces),
            ), ToolDef(
                name="qq_inspect_custom_face",
                description="用当前视觉模型查看一个收藏表情，保存可见内容和标签。想知道自己发过的表情是什么时也可查看。",
                parameters={"type": "object", "properties": {
                    "scope": {"type": "string", "enum": ["private", "group"]},
                    "target": {"type": "string"},
                    "emoji_id": {"type": "string"}},
                    "required": ["scope", "target", "emoji_id"], "additionalProperties": False},
                tags=(ToolTag.READ,),
                handler=self._owned_handler("qq_inspect_custom_face", self.inspect_custom_face),
            ), ToolDef(
                name="qq_look_image",
                description="主动观察当前会话的图片或旧图。默认查看全图，动图按时间格展示；想确认细节可指定毫秒或百分比局部。全图的客观描述会留到下次遇见，同一表情当下的意思请结合本次对话判断。",
                parameters={"type": "object", "properties": {
                    "scope": {"type": "string", "enum": ["private","group"]}, "target": {"type": "string"},
                    "media_id": {"type": "string"}, "question": {"type": "string", "maxLength": 500},
                    "at_ms": {"type": "integer", "minimum": 0},
                    "region": {"type": "array", "items": {"type":"integer","minimum":0,"maximum":100},
                               "minItems":4,"maxItems":4,"description":"左、上、右、下，百分比"}},
                    "required":["scope","target","media_id"], "additionalProperties":False},
                tags=(ToolTag.READ,), handler=self._owned_handler("qq_look_image", self.look_image),
            ), ToolDef(
                name="qq_correct_sticker",
                description="在控制台、私聊或群聊中，仅当本轮已核验的 owner 本人明确纠正刚才收到、收藏或发出的表情时，若能确定具体图片且认可角色、画面或通用梗义的纠正，调用此工具保存修订后的完整简短描述和标签，保留仍正确的部分。当前会话收到的图用 media_id，已发出的收藏用发送记录的 emoji_id。只属于这一次对话的语气或意图留在本轮，不写入通用标签；不确定指哪张图先问清。成功后才说已改。",
                parameters={"type":"object", "properties":{
                    "scope":{"type":"string","enum":["private","group"]}, "target":{"type":"string"},
                    "media_id":{"type":"string"}, "emoji_id":{"type":"string"},
                    "description":{"type":"string","minLength":1,"maxLength":300},
                    "tags":{"type":"array","minItems":1,"maxItems":10,
                            "items":{"type":"string","minLength":1,"maxLength":24}}},
                    "required":["scope","target","description","tags"], "additionalProperties":False},
                tags=(ToolTag.WRITE,), handler=self._owned_handler("qq_correct_sticker", self.correct_sticker),
            ), ToolDef(
                name="qq_collect_sticker",
                description="可自行收藏当前会话中 QQ 标为[表情包#编号]的来图，无须他人邀请或发言。[图片]、[图片文件]、[文件]和内置 QQ 表情不参与自主收藏，即使画面像梗图或是 GIF。先看清内容，再填写客观描述、短标签及自己喜欢它的理由。由你自行决定是否收藏，无须为了完成任务而收藏。按内容跨群去重。以工具回执为准：added_now=true 才表示这次新收藏了；关闭或重复时不能声称刚收藏，已有表情的添加来源见 added_by。",
                parameters={"type": "object", "properties": {
                    "scope": {"type": "string", "enum": ["private", "group"]},
                    "target": {"type": "string"},
                    "media_id": {"type": "string"},
                    "description": {"type": "string", "minLength": 1, "maxLength": 300},
                    "tags": {"type": "array", "items": {"type": "string", "maxLength": 24},
                             "minItems": 1, "maxItems": 10},
                    "reason": {"type": "string", "minLength": 1, "maxLength": 300}},
                    "required": ["scope", "target", "media_id", "description", "tags", "reason"],
                    "additionalProperties": False},
                tags=(ToolTag.WRITE,),
                handler=self._owned_handler("qq_collect_sticker", self.collect_sticker),
            ), ToolDef(
                name="qq_send_custom_face",
                description="发送当前 QQ 账号已收藏的表情包，需使用收藏表情的 emoji_id。",
                parameters={
                    "type": "object",
                    "properties": {
                        "scope": {"type": "string", "enum": ["private", "group"]},
                        "target": {"type": "string", "description": "纯数字 QQ 号或群号；不要带 qq: 或 group: 前缀"},
                        "emoji_id": {"type": "string", "description": "收藏表情的 emoji_id"},
                    },
                    "required": ["scope", "target", "emoji_id"],
                    "additionalProperties": False,
                },
                tags=(ToolTag.SPEAK,),
                handler=self._owned_handler("qq_send_custom_face", self.send_custom_face),
            ), ToolDef(
                name="qq_send_saved_image",
                description="把刚收到并保存的图片或 GIF 发回原 QQ 私聊或群聊。",
                parameters={
                    "type": "object",
                    "properties": {
                        "scope": {"type": "string", "enum": ["private", "group"]},
                        "target": {"type": "string", "description": "原纯数字 QQ 号或群号；不要带 qq: 或 group: 前缀"},
                        "media_id": {"type": "string", "description": "收到图片时显示的编号"},
                    },
                    "required": ["scope", "target", "media_id"],
                    "additionalProperties": False,
                },
                tags=(ToolTag.SPEAK,),
                handler=self._owned_handler("qq_send_saved_image", self.send_saved_image),
            ), ToolDef(
                name="qq_send_image",
                description="把一张插件生成的图片发到 QQ。image 收本机图片地址"
                            "（例如鸣潮卡片回执里的「卡片图」链接，必须是 PNG/JPEG/GIF/WebP，"
                "不是网页）或当前 Bot 独立 outbox 下的文件名；"
                            "远程网址与其它本地路径一律拒绝。可附一段文字。",
                parameters={
                    "type": "object",
                    "properties": {
                        "scope": {"type": "string", "enum": ["private", "group"]},
                        "target": {"type": "string", "description": "纯数字 QQ 号或群号；不要带前缀"},
                        "image": {"type": "string",
                                  "description": "本机图片地址（127.0.0.1）或 outbox 里的文件名"},
                        "caption": {"type": "string", "description": "可选的配文"},
                    },
                    "required": ["scope", "target", "image"],
                    "additionalProperties": False,
                },
                tags=(ToolTag.SPEAK,),
                handler=self._owned_handler("qq_send_image", self.send_image),
            ), ToolDef(
                name="qq_send_voice",
                description="用本机 TTS 合成语音并发往允许的 QQ 私聊或群聊。",
                parameters={
                    "type": "object",
                    "properties": {
                        "scope": {"type": "string", "enum": ["private", "group"]},
                        "target": {"type": "string", "description": "纯数字 QQ 号或群号；不要带 qq: 或 group: 前缀"},
                        "text": {"type": "string", "minLength": 1, "maxLength": 1000,
                                 "description": "要合成为语音的文字"},
                    },
                    "required": ["scope", "target", "text"],
                    "additionalProperties": False,
                },
                tags=(ToolTag.SPEAK,), handler=self._owned_handler('qq_send_voice', self.send_voice),
            )]

    return QQ()
