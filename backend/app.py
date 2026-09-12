#!/usr/bin/env python3
"""
companion relay backend — a private 1:1 message channel between a person and
their AI companion (an AI running locally as a Claude Code "channel" plugin).

Two ends, one shared secret:
  - AI side   (local CC channel plugin):  POST /channel/out  ·  SSE GET /channel/in
  - Human side (phone PWA):               POST /app/send     ·  SSE GET /app/stream  ·  GET /app/history

No framework magic: messages land in sqlite and fan out to SSE subscribers via
one asyncio.Queue per connection. A single shared Bearer secret guards every
endpoint (single user). The secret may travel in the Authorization header *or*
as a ?token= query param — because the browser's native EventSource cannot set
custom headers.

Everything personal — names, secrets, domain, paths — comes from environment
variables (see .env.example). Nothing identifying is hard-coded.
"""

import asyncio
import mimetypes
import hmac
import json
import os
import time
import re
import secrets
import subprocess
import sqlite3
import urllib.error
import urllib.request
import urllib.parse
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import FileResponse, Response, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

try:
    from pywebpush import webpush, WebPushException
except Exception:  # a missing lib must not stop the relay from starting
    webpush = None
    class WebPushException(Exception):
        pass


# --- identity (parameterized — set these to your own names) ----------------
AI_NAME = os.environ.get("RELAY_AI_NAME", "AI")          # AI companion's display name (push title, narration)
HUMAN_NAME = os.environ.get("RELAY_HUMAN_NAME", "对方")   # how the AI is told about you in voice/call narration

# --- core config / secrets (all from env) ----------------------------------
SECRET = os.environ.get("RELAY_SECRET", "")
DB_PATH = os.environ.get("RELAY_DB", str(Path(__file__).parent / "relay.db"))
PORT = int(os.environ.get("RELAY_PORT", "3011"))
UPLOAD_DIR = Path(os.environ.get("RELAY_UPLOAD_DIR", str(Path(__file__).parent / "uploads")))
PUBLIC_PREFIX = os.environ.get("RELAY_PUBLIC_PREFIX", "/relay").rstrip("/")
APP_PATH = os.environ.get("RELAY_APP_PATH", "/")  # where a push-notification tap opens the PWA
ALLOW_ORIGINS = [o.strip() for o in os.environ.get(
    "RELAY_ALLOW_ORIGINS", "http://localhost:8080,http://127.0.0.1:8080"
).split(",") if o.strip()]
MAX_UPLOAD_BYTES = int(os.environ.get("RELAY_MAX_UPLOAD_BYTES", str(10 * 1024 * 1024)))
VOICE_MAX_BYTES = int(os.environ.get("RELAY_VOICE_MAX_BYTES", str(8 * 1024 * 1024)))
VOICE_TRANSCRIBE_CMD = os.environ.get("RELAY_VOICE_TRANSCRIBE_CMD", "")

# --- MiniMax TTS (optional — leave keys blank to disable spoken replies) ----
MINIMAX_API_BASE = os.environ.get("MINIMAX_API_BASE", "https://api.minimaxi.com")
MINIMAX_API_KEY = os.environ.get("MINIMAX_API_KEY", "")
MINIMAX_GROUP_ID = os.environ.get("MINIMAX_GROUP_ID", "")
MINIMAX_MODEL = os.environ.get("MINIMAX_MODEL", "speech-02-hd")
MINIMAX_VOICE_ZH = os.environ.get("MINIMAX_VOICE_ZH", "")
MINIMAX_TTS_TIMEOUT = float(os.environ.get("MINIMAX_TTS_TIMEOUT", "30"))

# --- Web Push (VAPID, optional) — push unread replies to the PWA lock screen
VAPID_PUBLIC_KEY = os.environ.get("VAPID_PUBLIC_KEY", "")
VAPID_PRIVATE_PEM = os.environ.get("VAPID_PRIVATE_PEM", "")   # PEM file path OR inline PEM text
VAPID_SUBJECT = os.environ.get("VAPID_SUBJECT", "mailto:admin@example.com")
PUSH_PREVIEW_CHARS = int(os.environ.get("RELAY_PUSH_PREVIEW_CHARS", "120"))

# --- presence tuning (seconds) ---------------------------------------------
PRESENCE_ONLINE_SEC = int(os.environ.get("RELAY_PRESENCE_ONLINE_SEC", "180"))
PRESENCE_RECENT_SEC = int(os.environ.get("RELAY_PRESENCE_RECENT_SEC", "1800"))

# --- Optional server-side API loop -----------------------------------------
# "desktop" keeps the original Claude Code channel path. "loop" forwards new
# human messages to a local HTTP loop, which replies through /channel/out.
BRAIN_FILE = Path(os.environ.get("RELAY_BRAIN_FILE", str(Path(__file__).parent / "brain_target")))
LOOP_INGEST_URL = os.environ.get("RELAY_LOOP_INGEST_URL", "http://127.0.0.1:3020/loop/ingest")
STREAM_DRAFT_TTL = int(os.environ.get("RELAY_STREAM_DRAFT_TTL", "600"))

if not SECRET:
    raise SystemExit("RELAY_SECRET is required (set it in the systemd EnvironmentFile)")


# ---------------------------------------------------------------------------
# storage
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    with db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                ts        TEXT NOT NULL,
                direction TEXT NOT NULL,   -- 'in' (human -> AI) | 'out' (AI -> human)
                kind      TEXT NOT NULL,   -- 'user' | 'reply' | 'thinking' | 'voice' | 'call' | ...
                text      TEXT NOT NULL,
                meta      TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS push_subscriptions (
                endpoint TEXT PRIMARY KEY,
                p256dh   TEXT NOT NULL,
                auth     TEXT NOT NULL,
                ua       TEXT,
                created  TEXT NOT NULL,
                last_ok  TEXT
            )
            """
        )
        conn.commit()


def save_message(direction: str, kind: str, text: str, meta: dict) -> dict:
    ts = meta.get("ts") or now_iso()
    with db() as conn:
        cur = conn.execute(
            "INSERT INTO messages (ts, direction, kind, text, meta) VALUES (?,?,?,?,?)",
            (ts, direction, kind, text, json.dumps(meta, ensure_ascii=False)),
        )
        conn.commit()
        mid = cur.lastrowid
    if direction == "in":
        if proactive_state.get("circuit_broken"):
            proactive_state["circuit_broken"] = False
            proactive_state["last_error"] = ""
            print("[ProactiveWake] 人类发送新消息，已自动重置安全熔断器", flush=True)
    return {"id": mid, "ts": ts, "direction": direction, "kind": kind, "text": text, "meta": meta}


def set_reaction(message_id, who, emoji):
    # Set/clear one party's reaction on an existing message.
    # Returns the message's reactions dict, or None if the target doesn't exist.
    with db() as conn:
        row = conn.execute("SELECT meta FROM messages WHERE id = ?", (message_id,)).fetchone()
        if not row:
            return None
        meta = json.loads(row["meta"] or "{}")
        reactions = meta.get("reactions") or {}
        if emoji:
            reactions[who] = emoji
        else:
            reactions.pop(who, None)
        if reactions:
            meta["reactions"] = reactions
        else:
            meta.pop("reactions", None)
        conn.execute(
            "UPDATE messages SET meta = ? WHERE id = ?",
            (json.dumps(meta, ensure_ascii=False), message_id),
        )
        conn.commit()
    return reactions


def history(since: int, limit: int) -> list:
    with db() as conn:
        rows = conn.execute(
            "SELECT * FROM messages WHERE id > ? ORDER BY id ASC LIMIT ?",
            (since, limit),
        ).fetchall()
    return rows_to_messages(rows)


def history_for_session(session_id: str, since: int, limit: int) -> list:
    session_id = (session_id or "").strip()
    if not session_id:
        return history(since, limit)
    with db() as conn:
        if session_id == "__legacy__":
            rows = conn.execute(
                "SELECT * FROM messages "
                "WHERE id > ? AND (json_extract(meta, '$.api_session') IS NULL OR json_extract(meta, '$.api_session') = '') "
                "ORDER BY id ASC LIMIT ?",
                (since, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM messages "
                "WHERE id > ? AND json_extract(meta, '$.api_session') = ? "
                "ORDER BY id ASC LIMIT ?",
                (since, session_id, limit),
            ).fetchall()
    return rows_to_messages(rows)


def inbound_history(since: int, limit: int) -> list:
    with db() as conn:
        rows = conn.execute(
            "SELECT * FROM messages "
            "WHERE id > ? AND direction = 'in' "
            "AND (json_extract(meta, '$.triggered') IS NULL OR json_extract(meta, '$.triggered') != 0) "
            "ORDER BY id ASC LIMIT ?",
            (since, limit),
        ).fetchall()
    return rows_to_messages(rows)


def rows_to_messages(rows) -> list:
    return [
        {
            "id": r["id"], "ts": r["ts"], "direction": r["direction"],
            "kind": r["kind"], "text": r["text"], "meta": json.loads(r["meta"] or "{}"),
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# web push — subscription storage + send
# ---------------------------------------------------------------------------

def save_subscription(endpoint: str, p256dh: str, auth: str, ua: str = "") -> None:
    with db() as conn:
        conn.execute(
            """
            INSERT INTO push_subscriptions (endpoint, p256dh, auth, ua, created, last_ok)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(endpoint) DO UPDATE SET p256dh=excluded.p256dh, auth=excluded.auth, ua=excluded.ua
            """,
            (endpoint, p256dh, auth, ua, now_iso(), None),
        )
        conn.commit()


def delete_subscription(endpoint: str) -> None:
    with db() as conn:
        conn.execute("DELETE FROM push_subscriptions WHERE endpoint = ?", (endpoint,))
        conn.commit()


def list_subscriptions() -> list:
    with db() as conn:
        rows = conn.execute("SELECT endpoint, p256dh, auth FROM push_subscriptions").fetchall()
    return [{"endpoint": r["endpoint"], "keys": {"p256dh": r["p256dh"], "auth": r["auth"]}} for r in rows]


def mark_subscription_ok(endpoint: str) -> None:
    with db() as conn:
        conn.execute("UPDATE push_subscriptions SET last_ok = ? WHERE endpoint = ?", (now_iso(), endpoint))
        conn.commit()


def _send_one_push(sub: dict, data: str):
    """Blocking single send (run in a thread). Returns (endpoint, status): 0=ok, 404/410=dead, else=transient."""
    if webpush is None:
        return sub["endpoint"], -1
    try:
        webpush(
            subscription_info=sub,
            data=data,
            vapid_private_key=VAPID_PRIVATE_PEM,
            vapid_claims={"sub": VAPID_SUBJECT},
            timeout=10,
        )
        return sub["endpoint"], 0
    except WebPushException as exc:
        code = getattr(getattr(exc, "response", None), "status_code", 0) or 0
        return sub["endpoint"], code
    except Exception:
        return sub["endpoint"], -1


async def push_to_all(payload: dict) -> dict:
    """Best-effort fan-out to all subscriptions; never raises. 404/410 prunes dead subs."""
    if webpush is None or not VAPID_PUBLIC_KEY or not VAPID_PRIVATE_PEM:
        return {"sent": 0, "dead": 0, "skipped": "not_configured"}
    subs = list_subscriptions()
    if not subs:
        return {"sent": 0, "dead": 0}
    data = json.dumps(payload, ensure_ascii=False)
    results = await asyncio.gather(*[asyncio.to_thread(_send_one_push, s, data) for s in subs])
    sent = dead = 0
    for endpoint, status in results:
        if status == 0:
            sent += 1
            mark_subscription_ok(endpoint)
        elif status in (404, 410):
            delete_subscription(endpoint)
            dead += 1
    return {"sent": sent, "dead": dead}


_PUSH_TAG_RE = re.compile(r"<[^>]+>")


def notification_from_message(msg: dict) -> dict:
    raw = (msg.get("text") or "").strip()
    body = _PUSH_TAG_RE.sub("", raw)
    body = re.sub(r"\s+", " ", body).strip()
    if len(body) > PUSH_PREVIEW_CHARS:
        body = body[:PUSH_PREVIEW_CHARS].rstrip() + "…"
    if not body:
        body = f"{AI_NAME}给你发来一条消息"
    return {"title": AI_NAME, "body": body, "url": APP_PATH, "id": msg.get("id"), "ts": msg.get("ts")}


# ---------------------------------------------------------------------------
# pub/sub — one asyncio.Queue per connected SSE client
# ---------------------------------------------------------------------------

plugin_subs: set[asyncio.Queue] = set()  # AI side    (GET /channel/in)
app_subs: set[asyncio.Queue] = set()     # human side (GET /app/stream)
stream_drafts: dict[tuple[str, str], dict] = {}

# --- PWA 前台可见性状态与 Push 防抖 ---
_is_client_visible: bool = False
_last_visible_ts: datetime | None = None
_last_push_time: float = 0.0
PUSH_DEBOUNCE_SEC: float = 5.0  # 5秒内多条连续回复气泡只推送首条，避免轰炸震动


def should_push_notification() -> bool:
    """判断是否应当向客户端发送锁屏推送通知。
    1. 若没有任何前端 SSE 连接在线，必定推送。
    2. 若有 SSE 连接（如保活音乐常驻），但客户端不在前台（切走或锁屏），必定推送。
    3. 若最近 5 秒内刚刚推送过，防抖抑制，避免连发多条气泡连续震动。
    """
    global _last_push_time
    now = time.time()
    if now - _last_push_time < PUSH_DEBOUNCE_SEC:
        return False

    # 没有客户端在线，直接推
    if not app_subs:
        return True

    # 客户端虽保持连接，但处于后台（或超过 75 秒没有前台心跳确认可见）
    if not _is_client_visible:
        return True
    if _last_visible_ts is None:
        return True

    age = (datetime.now(timezone.utc) - _last_visible_ts).total_seconds()
    if age > 75.0:
        return True

    # 此时客户端在线且前台可见，不打扰正在看屏的用户
    return False


async def try_push_notification(msg: dict) -> None:
    """尝试为 AI 真实回复发送推送通知（带前台判定与防抖）。"""
    global _last_push_time
    if not should_push_notification():
        return
    _last_push_time = time.time()
    try:
        await push_to_all(notification_from_message(msg))
    except Exception:
        pass


async def broadcast(subs: set, payload: dict) -> None:
    for q in list(subs):
        try:
            q.put_nowait(payload)
        except asyncio.QueueFull:
            subs.discard(q)  # slow/dead consumer — drop it


def app_payload(msg: dict) -> dict:
    """Shape the PWA renders: from = 'human' | 'ai', plus kind for styling."""
    return {
        "id": msg["id"], "ts": msg["ts"],
        "from": "human" if msg["direction"] == "in" else "ai",
        "kind": msg["kind"], "text": msg["text"], "meta": msg["meta"],
    }


def plugin_payload(msg: dict) -> dict:
    meta = msg.get("meta") or {}
    p = {
        "id": msg["id"],
        "content": msg["text"],
        "user": meta.get("user") or "human",
        "ts": msg["ts"],
        "attachments": meta.get("attachments") or [],
    }
    if meta.get("llm_config"):
        p["llm_config"] = meta.get("llm_config")
    if meta.get("personas"):
        p["personas"] = meta.get("personas")
    if meta.get("time_context"):
        p["time_context"] = meta.get("time_context")
    if meta.get("memory_context"):
        p["memory_context"] = meta.get("memory_context")
    for f in ("tether_front", "tether_middle", "tether_back", "tether_context", "web_reader_enabled", "web_search_enabled", "weather_context", "inner_voice_prompt"):
        if meta.get(f) is not None:
            p[f] = meta.get(f)
    return p


def brain_target() -> str:
    try:
        target = BRAIN_FILE.read_text(encoding="utf-8").strip()
        return target if target in ("desktop", "loop") else "desktop"
    except FileNotFoundError:
        return "desktop"
    except Exception:
        return "desktop"


def _forward_to_loop_sync(msg: dict) -> None:
    meta = msg.get("meta") or {}
    data = json.dumps({
        "id": msg.get("id"),
        "text": msg.get("text", ""),
        "session_id": meta.get("api_session") or "",
    }, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        LOOP_INGEST_URL,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req, timeout=10).read()


async def forward_to_loop(msg: dict) -> None:
    try:
        await asyncio.to_thread(_forward_to_loop_sync, msg)
    except Exception as exc:
        print(f"[loop] forward failed: {type(exc).__name__}: {exc}")


def prune_stream_drafts() -> None:
    now = datetime.now(timezone.utc).timestamp()
    stale = [k for k, v in stream_drafts.items() if now - float(v.get("updated_at") or 0) > STREAM_DRAFT_TTL]
    for k in stale:
        stream_drafts.pop(k, None)


async def handle_stream_delta(kind: str, body: dict) -> dict:
    base_kind = kind[:-6] if kind.endswith("_delta") else kind
    if base_kind not in ("thinking", "reply"):
        raise HTTPException(status_code=400, detail="unknown stream kind")
    stream_id = str(body.get("stream_id") or "").strip()
    if not stream_id:
        raise HTTPException(status_code=400, detail="stream_id required")

    done = bool(body.get("done"))
    chunk = str(body.get("text") or "")
    meta = {k: v for k, v in body.items() if k not in ("type", "text", "done", "final_text")}
    meta["stream_id"] = stream_id
    key = (stream_id, base_kind)
    prune_stream_drafts()

    now_ts = datetime.now(timezone.utc).timestamp()
    draft = stream_drafts.get(key)
    if not draft:
        draft = {"text": "", "meta": meta, "ts": now_iso(), "updated_at": now_ts}
        stream_drafts[key] = draft
    draft["text"] += chunk
    if done and isinstance(body.get("final_text"), str):
        draft["text"] = body.get("final_text") or ""
    draft["meta"].update(meta)
    draft["updated_at"] = now_ts

    if not done:
        await broadcast(app_subs, {
            "type": kind,
            "stream_id": stream_id,
            "text": chunk,
            "done": False,
            "ts": draft["ts"],
            "api_session": draft["meta"].get("api_session") or "",
        })
        return {"ok": True, "stream_id": stream_id, "draft": True}

    text = draft.get("text") or ""
    stream_drafts.pop(key, None)
    if not text:
        return {"ok": True, "stream_id": stream_id, "saved": False}
    msg = save_message("out", base_kind, text, dict(draft.get("meta") or {}))
    await broadcast(app_subs, {"type": "typing", "active": False})
    await broadcast(app_subs, app_payload(msg))
    if base_kind == "reply":
        await try_push_notification(msg)
    return {"id": msg["id"], "stream_id": stream_id, "saved": True}


def loop_base_url() -> str:
    parsed = urllib.parse.urlparse(LOOP_INGEST_URL)
    if not parsed.scheme or not parsed.netloc:
        return "http://127.0.0.1:3020"
    return f"{parsed.scheme}://{parsed.netloc}"


def loop_json(path: str, method: str = "GET", body=None):
    data = None
    headers = {"Content-Type": "application/json"}
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(loop_base_url() + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=35) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")[:500]
        raise HTTPException(status_code=exc.code, detail=detail)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"loop proxy error: {exc}")


SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9_.-]+")


def clean_filename(name: str) -> str:
    name = Path(name or "file").name
    name = SAFE_NAME_RE.sub("_", name).strip("._") or "file"
    return name[:80]


def ext_for(name: str, mime: str) -> str:
    ext = Path(name).suffix.lower()
    if ext and re.fullmatch(r"\.[A-Za-z0-9]{1,8}", ext):
        return ext
    guessed = mimetypes.guess_extension((mime or "").split(";", 1)[0].strip())
    return guessed or ".bin"


def save_upload_bytes(data: bytes, name: str, mime: str, prefix: str = "att") -> dict:
    if len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="file too large")
    safe = clean_filename(name)
    ext = ext_for(safe, mime)
    stored = f"{prefix}-{secrets.token_urlsafe(10)}{ext}"
    path = UPLOAD_DIR / stored
    path.write_bytes(data)
    kind = "image" if (mime or "").startswith("image/") else ("audio" if (mime or "").startswith("audio/") else "file")
    return {
        "url": f"{PUBLIC_PREFIX}/uploads/{stored}" if PUBLIC_PREFIX else f"/uploads/{stored}",
        "name": safe,
        "size": len(data),
        "mime": mime or "application/octet-stream",
        "kind": kind,
    }


def transcribe_with_command(audio_path: Path, mime: str) -> str:
    """Optional local ASR hook. The command receives <audio_path> <mime> and prints a transcript."""
    if not VOICE_TRANSCRIBE_CMD:
        return ""
    try:
        proc = subprocess.run(
            [VOICE_TRANSCRIBE_CMD, str(audio_path), mime or "application/octet-stream"],
            text=True,
            capture_output=True,
            timeout=45,
            check=False,
        )
    except Exception:
        return ""
    if proc.returncode != 0:
        return ""
    return proc.stdout.strip()


def minimax_tts_mp3(text: str) -> bytes:
    if not MINIMAX_API_KEY or not MINIMAX_VOICE_ZH:
        raise HTTPException(status_code=503, detail="minimax tts not configured")
    clean = (text or "").strip()
    if not clean:
        raise HTTPException(status_code=400, detail="empty text")
    clean = clean[:900]
    url = f"{MINIMAX_API_BASE.rstrip('/')}/v1/t2a_v2"
    if MINIMAX_GROUP_ID:
        url += f"?GroupId={MINIMAX_GROUP_ID}"
    payload = {
        "model": MINIMAX_MODEL,
        "text": clean,
        "stream": False,
        "voice_setting": {
            "voice_id": MINIMAX_VOICE_ZH,
            "speed": 1.0,
            "vol": 1.0,
            "pitch": 0,
        },
        "audio_setting": {
            "sample_rate": 32000,
            "bitrate": 128000,
            "format": "mp3",
            "channel": 1,
        },
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {MINIMAX_API_KEY}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=MINIMAX_TTS_TIMEOUT) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"minimax tts failed: {exc}")
    audio_hex = (data.get("data") or {}).get("audio")
    if not audio_hex:
        raise HTTPException(status_code=502, detail="minimax tts returned no audio")
    try:
        return bytes.fromhex(audio_hex)
    except ValueError:
        raise HTTPException(status_code=502, detail="bad minimax audio payload")


def sse_data(payload: dict) -> str:
    lines: list[str] = []
    event_id = payload.get("id")
    if event_id is not None:
        lines.append(f"id: {event_id}")
    lines.append(f"data: {json.dumps(payload, ensure_ascii=False)}")
    return "\n".join(lines) + "\n\n"


def sse_ping() -> str:
    payload = {"type": "ping", "ts": datetime.now(timezone.utc).isoformat()}
    return "event: ping\n" + sse_data(payload)


async def sse_stream(subs: set, request: Request, initial: list[dict] | None = None):
    q: asyncio.Queue = asyncio.Queue(maxsize=1000)
    subs.add(q)
    try:
        yield "retry: 3000\n: connected\n\n"
        for payload in initial or []:
            yield sse_data(payload)
        while True:
            if await request.is_disconnected():
                break
            try:
                payload = await asyncio.wait_for(q.get(), timeout=5)
                yield sse_data(payload)
            except asyncio.TimeoutError:
                yield sse_ping()  # keep the connection alive and let clients watchdog it
    finally:
        subs.discard(q)


SSE_HEADERS = {
    "Cache-Control": "no-cache, no-transform",
    "X-Accel-Buffering": "no",  # tell nginx not to buffer the stream
    "Connection": "keep-alive",
}


# ---------------------------------------------------------------------------
# auth — one shared Bearer secret on every endpoint (single user)
# ---------------------------------------------------------------------------

def check_auth(request: Request) -> None:
    auth = request.headers.get("authorization", "")
    token = auth[7:] if auth.startswith("Bearer ") else request.query_params.get("token")
    if not token or not hmac.compare_digest(token, SECRET):
        raise HTTPException(status_code=401, detail="unauthorized")


# ---------------------------------------------------------------------------
# 主动打破沉默与定时唤醒 (Proactive Wake) 机制
# ---------------------------------------------------------------------------

PROACTIVE_CONFIG_FILE = Path(DB_PATH).parent / "proactive_config.json"

proactive_state = {
    "enabled": False,
    "interval_minutes": 180,
    "last_attempt_time": 0.0,
    "circuit_broken": False,  # 只要 API 失败 1 次立即熔断，静默挂起
    "last_error": "",
    "context": {}             # 缓存最近一次前端同步的上下文 (llm_config, personas 等)
}


def parse_iso_ts(ts_str: str) -> float:
    try:
        cleaned = (ts_str or "").replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned)
        return dt.timestamp()
    except Exception:
        return time.time()


def load_proactive_config():
    try:
        if PROACTIVE_CONFIG_FILE.exists():
            with open(PROACTIVE_CONFIG_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                proactive_state["enabled"] = bool(data.get("enabled", False))
                proactive_state["interval_minutes"] = max(1, int(data.get("interval_minutes", 180)))
                proactive_state["context"] = data.get("context", {})
                print(f"[ProactiveWake] 已载入配置: 开启={proactive_state['enabled']}, 间隔={proactive_state['interval_minutes']}m", flush=True)
    except Exception as e:
        print(f"[ProactiveWake] 读取配置文件异常: {e}", flush=True)


def save_proactive_config():
    try:
        with open(PROACTIVE_CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "enabled": proactive_state["enabled"],
                "interval_minutes": proactive_state["interval_minutes"],
                "context": proactive_state["context"]
            }, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[ProactiveWake] 保存配置文件异常: {e}", flush=True)


async def trigger_proactive_bundle(test_mode: bool = False) -> bool:
    if not plugin_subs:
        print("[ProactiveWake] 当前无 AI 桥接连接 (plugin_subs 为空)，跳过主动唤醒", flush=True)
        return False

    with db() as conn:
        max_r = conn.execute("SELECT MAX(id) as max_id FROM messages").fetchone()
        next_evt_id = ((max_r["max_id"] if max_r and max_r["max_id"] else 0) + 1)

    ctx = proactive_state.get("context") or {}
    bundle = {
        "type": "bundle",
        "event_id": next_evt_id,
        "content": "[系统指令 · 主动打破沉默与问候]",
        "proactive": True,
        "test_mode": test_mode,
        "created_at": now_iso(),
    }
    for k in ("llm_config", "personas", "time_context", "memory_context",
              "tether_front", "tether_middle", "tether_back", "tether_context",
              "web_reader_enabled", "web_search_enabled", "weather_context", "inner_voice_prompt"):
        if ctx.get(k) is not None:
            bundle[k] = ctx.get(k)

    await broadcast(plugin_subs, bundle)
    return True


async def proactive_wake_worker():
    """后台巡视任务：每 30 秒检查一次静默时长，失败立即熔断。"""
    load_proactive_config()
    while True:
        try:
            await asyncio.sleep(30)
            if not proactive_state["enabled"]:
                continue
            if proactive_state["circuit_broken"]:
                continue  # 已熔断，保持静默，等待人类发言或用户重新开关

            interval_sec = max(60, proactive_state["interval_minutes"] * 60)
            now = time.time()

            # 查数据库中最后一条消息的创建时间与方向
            with db() as conn:
                last_row = conn.execute(
                    "SELECT ts, direction, kind FROM messages ORDER BY id DESC LIMIT 1"
                ).fetchone()

            if not last_row:
                continue

            last_msg_time = parse_iso_ts(last_row["ts"])
            elapsed = now - last_msg_time

            # 检查静默时长是否达到设定值
            if elapsed < interval_sec:
                continue

            # 检查距离上一次尝试触发唤醒的时间，必须也至少过去 interval_sec（双重防抖）
            if (now - proactive_state["last_attempt_time"]) < interval_sec:
                continue

            proactive_state["last_attempt_time"] = now
            print(f"[ProactiveWake] 满足静默唤醒条件 (已静默 {int(elapsed/60)} 分钟 >= 设定 {proactive_state['interval_minutes']} 分钟)，触发主动打招呼...", flush=True)

            await trigger_proactive_bundle(test_mode=False)

        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[ProactiveWake] 巡视任务运行异常: {e}", flush=True)
            await asyncio.sleep(10)


# ---------------------------------------------------------------------------
# app
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    load_proactive_config()
    wake_task = asyncio.create_task(proactive_wake_worker())
    yield
    wake_task.cancel()


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/healthz")
async def healthz():
    return {"ok": True, "plugin_subs": len(plugin_subs), "app_subs": len(app_subs)}


# ---- AI side ---------------------------------------------------------------

@app.get("/channel/inbound_pending")
async def channel_inbound_pending(request: Request, since: int = 0, limit: int = 50):
    check_auth(request)
    return [plugin_payload(m) for m in inbound_history(since, min(limit, 100))]

@app.get("/channel/in")
async def channel_in(request: Request, since: int = 0, limit: int = 100):
    """SSE stream the plugin holds open. The human's messages get pushed down here."""
    check_auth(request)
    backlog = [plugin_payload(m) for m in inbound_history(since, min(limit, 500))]
    return StreamingResponse(sse_stream(plugin_subs, request, backlog), media_type="text/event-stream", headers=SSE_HEADERS)


@app.post("/channel/out")
async def channel_out(request: Request):
    """The AI's reply/react. Persist + fan out to the PWA."""
    check_auth(request)
    body = await request.json()
    kind = body.get("type", "reply")
    if kind in ("thinking_delta", "reply_delta"):
        return await handle_stream_delta(kind, body)
    if kind == "react":
        # An emoji reaction attached to an existing message's meta.reactions; no new
        # message is created. An empty emoji clears that reaction.
        try:
            target_id = int(body.get("id"))
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="react: numeric id required")
        emoji = (body.get("emoji") or "").strip()
        reactions = set_reaction(target_id, "ai", emoji)
        if reactions is None:
            raise HTTPException(status_code=404, detail="react: message not found")
        await broadcast(app_subs, {"type": "reaction", "id": target_id, "reactions": reactions, "by": "ai"})
        # A react is also the AI "acting" once — clear the typing indicator so the
        # header doesn't stay stuck typing when no reply follows.
        await broadcast(app_subs, {"type": "typing", "active": False})
        return {"id": target_id, "reactions": reactions}
    text = body.get("text", "")
    meta = {k: v for k, v in body.items() if k not in ("type", "text")}
    msg = save_message("out", kind, text, meta)
    # the AI replied — clear the typing state
    await broadcast(app_subs, {"type": "typing", "active": False})
    await broadcast(app_subs, app_payload(msg))
    if kind == "reply":
        await try_push_notification(msg)
    return {"id": msg["id"]}


# ---- human side ------------------------------------------------------------


@app.post("/app/fetch_models")
async def fetch_models_proxy(request: Request):
    """Proxy /models request from frontend to bypass browser CORS."""
    check_auth(request)
    body = await request.json()
    base_url = (body.get("base_url") or "").rstrip("/")
    api_key = body.get("api_key") or ""
    if not base_url:
        raise HTTPException(status_code=400, detail="base_url required")
    target_url = base_url if base_url.endswith("/models") else f"{base_url}/models"
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    req = urllib.request.Request(target_url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data
    except urllib.error.HTTPError as exc:
        raise HTTPException(status_code=exc.code, detail=f"HTTP {exc.code}")
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@app.post("/app/proxy_completion")
async def app_proxy_completion(request: Request):
    """Proxy chat completion to bypass browser CORS (e.g. for generating summary)."""
    check_auth(request)
    body = await request.json()
    base_url = (body.get("base_url") or "").rstrip("/")
    api_key = body.get("api_key") or ""
    messages = body.get("messages") or []
    model = body.get("model") or ""
    temperature = body.get("temperature", 0.7)
    if not base_url or not model:
        raise HTTPException(status_code=400, detail="base_url and model required")
    target_url = f"{base_url}/chat/completions"
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    payload = json.dumps({
        "model": model,
        "messages": messages,
        "temperature": temperature
    }, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(target_url, data=payload, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data
    except urllib.error.HTTPError as exc:
        err_body = exc.read().decode("utf-8", "replace")[:500]
        raise HTTPException(status_code=exc.code, detail=f"HTTP {exc.code}: {err_body}")
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@app.post("/app/trigger_reply")
async def app_trigger_reply(request: Request):
    """Explicitly trigger AI response for pending user message(s)."""
    check_auth(request)
    body = await request.json()

    # 1. 查找上一次 AI 回复之后的所有未回复人类消息 (仅认成功的 reply，报错不阻断重试)
    with db() as conn:
        last_ai_row = conn.execute(
            "SELECT MAX(id) as max_id FROM messages WHERE direction = 'out' AND kind = 'reply'"
        ).fetchone()
        last_ai_id = last_ai_row["max_id"] if (last_ai_row and last_ai_row["max_id"]) else 0
        rows = conn.execute(
            "SELECT * FROM messages WHERE id > ? AND direction = 'in' ORDER BY id ASC",
            (last_ai_id,)
        ).fetchall()

    pending_msgs = rows_to_messages(rows)

    # 兜底：如果没找到（比如刚清空历史，或者只有人类消息），则取最新一条人类消息
    if not pending_msgs:
        with db() as conn:
            row = conn.execute(
                "SELECT * FROM messages WHERE direction = 'in' ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                pending_msgs = rows_to_messages([row])

    if not pending_msgs:
        raise HTTPException(status_code=400, detail="no human message to reply to")

    # 2. 将这些待回复的消息标记为 triggered = True，防止重复或状态混乱
    msg_ids = [m["id"] for m in pending_msgs]
    with db() as conn:
        for mid in msg_ids:
            row = conn.execute("SELECT meta FROM messages WHERE id = ?", (mid,)).fetchone()
            if row:
                m_meta = json.loads(row["meta"] or "{}")
                m_meta["triggered"] = True
                conn.execute(
                    "UPDATE messages SET meta = ? WHERE id = ?",
                    (json.dumps(m_meta, ensure_ascii=False), mid)
                )
        conn.commit()

    # 3. 组装 bundle 打包负载广播给 bridge (AI 侧)
    with db() as conn:
        max_r = conn.execute("SELECT MAX(id) as max_id FROM messages").fetchone()
        next_evt_id = ((max_r["max_id"] if max_r and max_r["max_id"] else 0) + 1)

    bundle = {
        "type": "bundle",
        "id": next_evt_id,
        "content": "\n".join(m["text"] for m in pending_msgs if m.get("text")),
        "items": [plugin_payload(m) for m in pending_msgs],
        "user": "human",
        "attachments": (pending_msgs[-1].get("meta") or {}).get("attachments") if pending_msgs else [],
    }
    if body.get("llm_config"):
        bundle["llm_config"] = body.get("llm_config")
    if body.get("personas"):
        bundle["personas"] = body.get("personas")
    if body.get("time_context"):
        bundle["time_context"] = body.get("time_context")
    if body.get("memory_context"):
        bundle["memory_context"] = body.get("memory_context")
    for f in ("tether_front", "tether_middle", "tether_back", "tether_context", "web_reader_enabled", "web_search_enabled", "weather_context", "inner_voice_prompt"):
        if body.get(f) is not None:
            bundle[f] = body.get(f)

    if brain_target() == "loop":
        loop_msg = dict(pending_msgs[-1])
        loop_msg["text"] = bundle["content"]
        asyncio.create_task(forward_to_loop(loop_msg))
    if plugin_subs:
        await broadcast(plugin_subs, bundle)
    await broadcast(app_subs, {"type": "typing", "active": True})
    return {"ok": True, "triggered_ids": msg_ids}


@app.post("/app/reroll")
async def app_reroll(request: Request):
    """自动清理最后一次 AI 回复的所有消息，并自动重新触发生成。"""
    check_auth(request)
    body = await request.json()
    delete_ids = body.get("delete_ids") or []

    # 1. 扫描末尾连续的所有 AI 消息 (direction = 'out')，彻底清除旧回复
    tail_ai_ids = []
    with db() as conn:
        rows = conn.execute("SELECT id, direction FROM messages ORDER BY id DESC").fetchall()
        for r in rows:
            if r["direction"] == "out":
                tail_ai_ids.append(r["id"])
            else:
                break

    to_delete = list(set([int(x) for x in delete_ids if str(x).isdigit()] + tail_ai_ids))
    if to_delete:
        with db() as conn:
            placeholders = ",".join("?" for _ in to_delete)
            conn.execute(f"DELETE FROM messages WHERE id IN ({placeholders})", to_delete)
            conn.commit()
        # 通知所有前端窗口清理对应的 DOM 气泡与本地缓存
        await broadcast(app_subs, {"type": "messages_deleted", "ids": to_delete})

    # 2. 查找上一次 AI 回复之后的所有未回复人类消息 (即重试的目标问题)
    with db() as conn:
        last_ai_row = conn.execute(
            "SELECT MAX(id) as max_id FROM messages WHERE direction = 'out' AND kind = 'reply'"
        ).fetchone()
        last_ai_id = last_ai_row["max_id"] if (last_ai_row and last_ai_row["max_id"]) else 0
        rows = conn.execute(
            "SELECT * FROM messages WHERE id > ? AND direction = 'in' ORDER BY id ASC",
            (last_ai_id,)
        ).fetchall()

    pending_msgs = rows_to_messages(rows)

    # 兜底：如果没找到，取最新一条人类消息
    if not pending_msgs:
        with db() as conn:
            row = conn.execute(
                "SELECT * FROM messages WHERE direction = 'in' ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                pending_msgs = rows_to_messages([row])

    if not pending_msgs:
        await broadcast(app_subs, {"type": "typing", "active": False})
        return {"ok": False, "detail": "没有可重新回复的人类消息"}

    # 3. 先标记 triggered = True，再广播 sync_history，保证 bridge 拉 inbound_pending 时能查到
    msg_ids = [m["id"] for m in pending_msgs]
    with db() as conn:
        for mid in msg_ids:
            row = conn.execute("SELECT meta FROM messages WHERE id = ?", (mid,)).fetchone()
            if row:
                m_meta = json.loads(row["meta"] or "{}")
                m_meta["triggered"] = True
                conn.execute(
                    "UPDATE messages SET meta = ? WHERE id = ?",
                    (json.dumps(m_meta, ensure_ascii=False), mid)
                )
        conn.commit()

    # 4. 组装 sync_history 帧，直接附带待重试的消息 items 与所有上下文配置，
    #    bridge 收到后重置历史并立即直接生成，无需额外 HTTP 往返查库。
    sync_frame = {
        "type": "sync_history",
        "items": [plugin_payload(m) for m in pending_msgs],
    }
    for f in ("llm_config", "personas", "time_context", "memory_context",
              "tether_front", "tether_middle", "tether_back", "tether_context",
              "web_reader_enabled", "web_search_enabled", "weather_context", "inner_voice_prompt"):
        if body.get(f) is not None:
            sync_frame[f] = body.get(f)

    if brain_target() == "loop":
        # loop 模式走原有路径
        bundle_content = "\n".join(m["text"] for m in pending_msgs if m.get("text"))
        loop_msg = dict(pending_msgs[-1])
        loop_msg["text"] = bundle_content
        asyncio.create_task(forward_to_loop(loop_msg))
    if plugin_subs:
        await broadcast(plugin_subs, sync_frame)

    await broadcast(app_subs, {"type": "typing", "active": True})
    return {"ok": True, "deleted_ids": to_delete, "triggered_ids": msg_ids}


@app.post("/app/messages/batch_delete")
async def batch_delete_messages(request: Request):
    """Batch delete messages from server database and sync with AI."""
    check_auth(request)
    body = await request.json()
    ids = body.get("ids") or []
    valid_ids = [int(x) for x in ids if str(x).isdigit() and int(x) > 0]
    if valid_ids:
        with db() as conn:
            placeholders = ",".join("?" for _ in valid_ids)
            conn.execute(f"DELETE FROM messages WHERE id IN ({placeholders})", valid_ids)
            conn.commit()
        await broadcast(app_subs, {"type": "messages_deleted", "ids": valid_ids})
        await broadcast(plugin_subs, {"type": "sync_history"})
    return {"ok": True, "deleted_ids": valid_ids}


@app.delete("/app/messages/{msg_id}")
async def delete_single_message(request: Request, msg_id: int):
    """Delete a single message from server database."""
    check_auth(request)
    with db() as conn:
        conn.execute("DELETE FROM messages WHERE id = ?", (msg_id,))
        conn.commit()
    # 广播通知所有前端窗口清理本地气泡，并通知 bridge 彻底重新对齐上下文记忆（绝不读取被删消息）
    await broadcast(app_subs, {"type": "messages_deleted", "ids": [msg_id]})
    await broadcast(plugin_subs, {"type": "sync_history"})
    return {"ok": True, "deleted_id": msg_id}


@app.patch("/app/messages/{msg_id}")
async def edit_single_message(request: Request, msg_id: int):
    """Edit text or role of a single message in server database."""
    check_auth(request)
    body = await request.json()
    new_text = body.get("text")
    new_from = body.get("from")
    with db() as conn:
        row = conn.execute("SELECT * FROM messages WHERE id = ?", (msg_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="message not found")
        updates = []
        params = []
        if new_text is not None:
            updates.append("text = ?")
            params.append(str(new_text))
        if new_from is not None:
            new_dir = "in" if new_from == "human" else "out"
            updates.append("direction = ?")
            params.append(new_dir)
        if updates:
            params.append(msg_id)
            conn.execute(f"UPDATE messages SET {', '.join(updates)} WHERE id = ?", params)
            conn.commit()
    # 消息修改后，通知 bridge 重新加载对齐最新上下文
    await broadcast(plugin_subs, {"type": "sync_history"})
    return {"ok": True, "id": msg_id, "text": new_text, "from": new_from}


@app.post("/app/messages/insert")
async def insert_single_message(request: Request):
    """Insert a message at a specific timestamp (between existing messages)."""
    check_auth(request)
    body = await request.json()
    text = (body.get("text") or "").strip()
    role = body.get("from") or "human"
    ts = body.get("ts") or now_iso()
    kind = body.get("kind") or ("user" if role == "human" else "reply")
    direction = "in" if role == "human" else "out"
    meta = body.get("meta") if isinstance(body.get("meta"), dict) else {}
    if not text:
        raise HTTPException(status_code=400, detail="empty text")
    with db() as conn:
        cur = conn.execute(
            "INSERT INTO messages (ts, direction, kind, text, meta) VALUES (?,?,?,?,?)",
            (ts, direction, kind, text, json.dumps(meta, ensure_ascii=False)),
        )
        conn.commit()
        mid = cur.lastrowid
    # 插入新消息后，通知 bridge 重新对齐上下文
    await broadcast(plugin_subs, {"type": "sync_history"})
    return {"ok": True, "message": {"id": mid, "ts": ts, "from": role, "kind": kind, "text": text, "meta": meta}}


@app.post("/app/clear_history")
async def clear_all_history(request: Request):
    """Clear all messages from server database and reset cursor."""
    check_auth(request)
    with db() as conn:
        conn.execute("DELETE FROM messages")
        conn.commit()
    # Also notify connected plugin and app tabs
    await broadcast(app_subs, {"type": "clear_history"})
    await broadcast(plugin_subs, {"type": "clear_history"})
    return {"ok": True, "cleared": True}

@app.post("/app/send")
async def app_send(request: Request):
    """Human types in the PWA. Persist, push to the AI (plugin), echo to other PWA tabs."""
    check_auth(request)
    body = await request.json()
    text = (body.get("text") or "").strip()
    attachments = body.get("attachments") if isinstance(body.get("attachments"), list) else []
    api_session = str(body.get("api_session") or body.get("session_id") or "").strip()
    if not text and not attachments:
        raise HTTPException(status_code=400, detail="empty text")
    trigger = body.get("trigger", True)
    meta = {"user": "human", "attachments": attachments, "triggered": trigger}
    if api_session:
        meta["api_session"] = api_session
    if body.get("llm_config"):
        meta["llm_config"] = body.get("llm_config")
    if body.get("personas"):
        meta["personas"] = body.get("personas")
    if body.get("time_context"):
        meta["time_context"] = body.get("time_context")
    if body.get("memory_context"):
        meta["memory_context"] = body.get("memory_context")
    for f in ("tether_front", "tether_middle", "tether_back", "tether_context", "web_reader_enabled", "web_search_enabled", "weather_context", "inner_voice_prompt"):
        if body.get(f) is not None:
            meta[f] = body.get(f)
    msg = save_message("in", "user", text, meta)
    # echo to the PWA so the sender's bubble + other tabs stay in sync
    await broadcast(app_subs, app_payload(msg))
    if trigger:
        # Route to exactly one AI body. "desktop" keeps the Claude Code channel;
        # "loop" calls the optional server-side API loop.
        if brain_target() == "loop":
            asyncio.create_task(forward_to_loop(msg))
        if plugin_subs:
            await broadcast(plugin_subs, plugin_payload(msg))
        # the AI starts processing -> push a typing state to the PWA
        await broadcast(app_subs, {"type": "typing", "active": True})
    return {"id": msg["id"]}


@app.post("/app/upload")
async def app_upload(request: Request, name: str = "file"):
    check_auth(request)
    data = await request.body()
    if not data:
        raise HTTPException(status_code=400, detail="empty file")
    mime = request.headers.get("content-type", "application/octet-stream")
    return save_upload_bytes(data, name, mime, "att")


@app.get("/relay/uploads/{name}")
@app.get("/uploads/{name}")
async def uploads(request: Request, name: str):
    check_auth(request)
    safe = clean_filename(name)
    path = UPLOAD_DIR / safe
    if not path.exists() or not path.is_file():
        # 兼容去掉静态扩展名的请求（彻底绕过 Nginx/宝塔 location ~ \.(jpg|png)$ 静态资源规则劫持）
        matches = list(UPLOAD_DIR.glob(f"{safe}.*"))
        if matches and matches[0].is_file():
            path = matches[0]
        else:
            raise HTTPException(status_code=404, detail="not found")
    return FileResponse(path)


@app.post("/app/voice")
async def app_voice(request: Request):
    """Voice input from the PWA. Prefer the browser transcript; fall back to an audio attachment."""
    check_auth(request)
    ctype = request.headers.get("content-type", "")

    if ctype.startswith("application/json"):
        body = await request.json()
        transcript = (body.get("text") or body.get("transcript") or "").strip()
        if not transcript:
            raise HTTPException(status_code=400, detail="empty transcript")
        if not transcript.startswith("🎤"):
            transcript = "🎤 " + transcript
        meta = {"user": "human", "voice": True, "source": body.get("source") or "browser_speech"}
        msg = save_message("in", "voice", transcript, meta)
        await broadcast(plugin_subs, plugin_payload(msg))
        await broadcast(app_subs, app_payload(msg))
        await broadcast(app_subs, {"type": "typing", "active": True})
        return {"id": msg["id"], "text": transcript}

    data = await request.body()
    if not data:
        raise HTTPException(status_code=400, detail="empty audio")
    if len(data) > VOICE_MAX_BYTES:
        raise HTTPException(status_code=413, detail="voice too large")

    mime = ctype or "audio/webm"
    upload = save_upload_bytes(data, request.query_params.get("name", "voice.webm"), mime, "voice")
    stored = Path(upload["url"]).name
    local_audio = UPLOAD_DIR / stored
    transcript = transcribe_with_command(local_audio, mime)
    text = ("🎤 " + transcript) if transcript else f"🎤 [语音] {HUMAN_NAME}发来一段语音；当前 relay 未配置 ASR，音频已作为附件送达。"
    meta = {
        "user": "human",
        "voice": True,
        "source": "media_recorder",
        "attachments": [upload],
        "transcribed": bool(transcript),
    }
    msg = save_message("in", "voice", text, meta)
    await broadcast(plugin_subs, plugin_payload(msg))
    await broadcast(app_subs, app_payload(msg))
    await broadcast(app_subs, {"type": "typing", "active": True})
    return {"id": msg["id"], "text": transcript, "attachment": upload}


@app.post("/app/call")
async def app_call(request: Request):
    """Call lifecycle events from the PWA so the AI knows this is voice, not typing."""
    check_auth(request)
    body = await request.json()
    action = (body.get("action") or "").strip().lower()
    call_id = (body.get("call_id") or "").strip()
    if action not in {"start", "end"}:
        raise HTTPException(status_code=400, detail="invalid call action")
    if action == "start":
        text = f"📞 [call_start] {HUMAN_NAME}开启了语音通话。接下来带 🎤 的消息来自语音。请用适合朗读的短句回复。"
    else:
        text = f"📞 [call_end] {HUMAN_NAME}结束了语音通话。"
    msg = save_message("in", "call", text, {"user": "human", "call": action, "call_id": call_id})
    if action == "end":
        await broadcast(plugin_subs, plugin_payload(msg))
    if action == "start":
        await broadcast(app_subs, {"type": "typing", "active": True})
    return {"id": msg["id"]}


@app.post("/app/tts")
async def app_tts(request: Request):
    """Generate MiniMax speech for an AI reply. The frontend falls back if unavailable."""
    check_auth(request)
    body = await request.json()
    audio = minimax_tts_mp3(body.get("text") or "")
    return Response(
        content=audio,
        media_type="audio/mpeg",
        headers={"Cache-Control": "no-store"},
    )


# ---------------------------------------------------------------------------
# presence — the PWA POSTs /app/ping every ~60s; read /app/status to decide
# whether the human is around. In-memory only: a relay restart clears last_seen
# (state degrades to 'unknown') until the next ping.
# ---------------------------------------------------------------------------

_last_seen_ts = None


def _presence_state(now):
    if _last_seen_ts is None:
        return "unknown", None
    age = (now - _last_seen_ts).total_seconds()
    if age < PRESENCE_ONLINE_SEC:
        return "online", age
    if age < PRESENCE_RECENT_SEC:
        return "recent", age
    return "away", age


def latest_message():
    """Newest real conversational message (excludes 'thinking' stream)."""
    with db() as conn:
        row = conn.execute(
            "SELECT * FROM messages WHERE kind != 'thinking' ORDER BY id DESC LIMIT 1"
        ).fetchone()
    if not row:
        return None
    return rows_to_messages([row])[0]


@app.post("/app/ping")
async def app_ping(request: Request):
    """PWA foreground heartbeat."""
    check_auth(request)
    global _last_seen_ts, _last_visible_ts, _is_client_visible
    now = datetime.now(timezone.utc)
    _last_seen_ts = now
    _last_visible_ts = now
    _is_client_visible = True
    return {"ok": True}


@app.post("/app/visibility")
async def app_visibility(request: Request):
    """PWA 上报前台可见性（切出切回即时更新）。"""
    check_auth(request)
    global _is_client_visible, _last_visible_ts
    try:
        body = await request.json()
        visible = bool(body.get("visible", False))
    except Exception:
        visible = False
    _is_client_visible = visible
    if visible:
        _last_visible_ts = datetime.now(timezone.utc)
    return {"ok": True, "visible": _is_client_visible}


@app.post("/app/proactive_config")
async def update_proactive_config(request: Request):
    """更新主动打破沉默配置并同步上下文。"""
    check_auth(request)
    body = await request.json()
    if "enabled" in body:
        proactive_state["enabled"] = bool(body["enabled"])
    if "interval_minutes" in body:
        proactive_state["interval_minutes"] = max(1, int(body["interval_minutes"]))
    if "context" in body and isinstance(body["context"], dict):
        proactive_state["context"].update(body["context"])
    # 用户主动修改或开关，自动解除熔断
    proactive_state["circuit_broken"] = False
    proactive_state["last_error"] = ""
    save_proactive_config()
    return {
        "ok": True,
        "enabled": proactive_state["enabled"],
        "interval_minutes": proactive_state["interval_minutes"],
        "circuit_broken": proactive_state["circuit_broken"]
    }


@app.get("/app/proactive_config")
async def get_proactive_config_api(request: Request):
    """获取当前主动唤醒运行状态。"""
    check_auth(request)
    return {
        "ok": True,
        "enabled": proactive_state["enabled"],
        "interval_minutes": proactive_state["interval_minutes"],
        "circuit_broken": proactive_state["circuit_broken"],
        "last_error": proactive_state["last_error"]
    }


@app.post("/app/proactive_test")
async def test_proactive_wake(request: Request):
    """手动测试唤醒一次。"""
    check_auth(request)
    body = await request.json()
    if "context" in body and isinstance(body["context"], dict):
        proactive_state["context"].update(body["context"])
    proactive_state["circuit_broken"] = False
    success = await trigger_proactive_bundle(test_mode=True)
    return {"ok": success}


@app.post("/app/proactive_report")
async def report_proactive_result(request: Request):
    """Bridge 上报主动唤醒调用结果，失败时立即开启安全熔断。"""
    check_auth(request)
    body = await request.json()
    success = body.get("success", False)
    if not success:
        proactive_state["circuit_broken"] = True
        proactive_state["last_error"] = body.get("error", "API failure")
        print(f"[ProactiveWake] 收到 Bridge API 失败上报，触发熔断保护，暂停唤醒: {proactive_state['last_error']}", flush=True)
    else:
        print("[ProactiveWake] 主动打招呼成功完成", flush=True)
    return {"ok": True, "circuit_broken": proactive_state["circuit_broken"]}


@app.get("/app/status")
async def app_status(request: Request):
    """Presence state + the time/direction of the most recent message. Metadata only, no message text."""
    check_auth(request)
    now = datetime.now(timezone.utc)
    state, seen_age = _presence_state(now)
    last_msg = latest_message()
    last_msg_ts = last_msg["ts"] if last_msg else None
    last_msg_dir = last_msg["direction"] if last_msg else None
    last_msg_age = None
    if last_msg_ts:
        try:
            mt = datetime.fromisoformat(last_msg_ts)
            if mt.tzinfo is None:
                mt = mt.replace(tzinfo=timezone.utc)
            last_msg_age = (now - mt).total_seconds()
        except Exception:
            last_msg_age = None
    return {
        "now": now.isoformat(),
        "last_seen": _last_seen_ts.isoformat() if _last_seen_ts else None,
        "seen_age_sec": seen_age,
        "online": state == "online",
        "state": state,
        "last_msg_ts": last_msg_ts,
        "last_msg_dir": last_msg_dir,
        "last_msg_age_sec": last_msg_age,
    }


@app.get("/app/history")
async def app_history(request: Request, since: int = 0, limit: int = 200, session_id: str = ""):
    check_auth(request)
    rows = history_for_session(session_id, since, min(limit, 500)) if session_id else history(since, min(limit, 500))
    return {"messages": [app_payload(m) for m in rows]}


@app.get("/app/stream")
async def app_stream(request: Request):
    """SSE stream the PWA holds open while foregrounded. The AI's messages arrive here."""
    check_auth(request)
    return StreamingResponse(sse_stream(app_subs, request), media_type="text/event-stream", headers=SSE_HEADERS)


# ---- web push subscription management --------------------------------------

@app.get("/app/vapid_public")
async def app_vapid_public(request: Request):
    """Public key the PWA needs to subscribe (not a secret — safe to expose)."""
    check_auth(request)
    return {"key": VAPID_PUBLIC_KEY}


@app.post("/app/subscribe")
async def app_subscribe(request: Request):
    """PWA turns on lock-screen notifications: store the subscription."""
    check_auth(request)
    body = await request.json()
    endpoint = (body.get("endpoint") or "").strip()
    keys = body.get("keys") or {}
    p256dh = (keys.get("p256dh") or "").strip()
    auth = (keys.get("auth") or "").strip()
    if not endpoint or not p256dh or not auth:
        raise HTTPException(status_code=400, detail="endpoint + keys.p256dh + keys.auth required")
    ua = request.headers.get("user-agent", "")[:200]
    save_subscription(endpoint, p256dh, auth, ua)
    return {"ok": True, "count": len(list_subscriptions())}


@app.post("/app/unsubscribe")
async def app_unsubscribe(request: Request):
    """PWA turns off lock-screen notifications: drop the subscription."""
    check_auth(request)
    body = await request.json()
    endpoint = (body.get("endpoint") or "").strip()
    if endpoint:
        delete_subscription(endpoint)
    return {"ok": True}


@app.post("/app/push_test")
async def app_push_test(request: Request):
    """Self-test: push one test notification to every subscription."""
    check_auth(request)
    try:
        body = await request.json()
    except Exception:
        body = {}
    text = (body.get("text") if isinstance(body, dict) else None) or f"测试通知 · {AI_NAME}在这儿"
    res = await push_to_all({"title": AI_NAME, "body": text, "url": APP_PATH, "id": 0})
    return {"ok": True, **res}


# ---- optional API loop control --------------------------------------------

@app.get("/app/brain")
async def get_brain(request: Request):
    check_auth(request)
    return {"target": brain_target()}


@app.post("/app/brain")
async def set_brain(request: Request):
    check_auth(request)
    body = await request.json()
    target = str(body.get("target") or "").strip()
    if target not in ("desktop", "loop"):
        raise HTTPException(status_code=400, detail="target must be 'desktop' or 'loop'")
    BRAIN_FILE.write_text(target, encoding="utf-8")
    return {"target": target}


@app.get("/app/loop_config")
async def get_loop_config(request: Request):
    check_auth(request)
    return loop_json("/loop/config")


@app.post("/app/loop_config")
async def set_loop_config(request: Request):
    check_auth(request)
    return loop_json("/loop/config", method="POST", body=await request.json())


@app.get("/app/sessions")
async def app_sessions(request: Request):
    check_auth(request)
    return loop_json("/loop/sessions")


@app.post("/app/sessions")
async def app_sessions_create(request: Request):
    check_auth(request)
    body = await request.json()
    if "since_id" not in body:
        try:
            with db() as conn:
                row = conn.execute("SELECT MAX(id) AS id FROM messages").fetchone()
                body["since_id"] = int(row["id"] or 0)
        except Exception:
            body["since_id"] = 0
    return loop_json("/loop/sessions", method="POST", body=body)


@app.patch("/app/sessions/{session_id}")
async def app_sessions_patch(session_id: str, request: Request):
    check_auth(request)
    return loop_json(f"/loop/sessions/{urllib.parse.quote(session_id)}", method="PATCH", body=await request.json())


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=PORT)
