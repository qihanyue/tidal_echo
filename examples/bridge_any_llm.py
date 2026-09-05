#!/usr/bin/env python3
"""
bridge_any_llm.py — 把「任意 LLM API」接到 companion relay 的 AI 侧 bridge。

这是 channel/ 插件(Claude Code 专用)的通用替代品:不依赖 Claude Code,
用任何 OpenAI 兼容的模型(GPT / DeepSeek / Gemini / GLM / Kimi / 通义 / 本地
vLLM …)当「AI 大脑」。前端 PWA 和 relay 后端原样不动。

它是个「带工具的聊天」循环,不是会自己乱跑的自主 agent —— 只在收到人类
消息时动一次:

    ① SSE 长连  GET  {RELAY}/channel/in?since={cursor}   收人类消息(实时)
    ② 用内存维护的近期对话 + persona(system),调你的模型(OpenAI 格式)
    ③ POST       {RELAY}/channel/out  {"type":"reply","text":...}   回复回手机

首次启动会拉一次历史做「暖启动」上下文,并把游标设到当前最新一条 —— 所以
**不会回放/重答你过去的旧消息**,只应答启动之后的新消息。重启则从上次游标
继续,补答断线期间漏掉的。

零第三方依赖(只用 Python 标准库,3.7+)。配置全走环境变量,可放在同目录
.env(见 .env.example)。跑起来:

    cp .env.example .env   &&   # 填好 RELAY_URL / RELAY_SECRET / LLM_* 三件
    python3 bridge_any_llm.py

⚠️ 单身体原则:同一时刻只跑一个 AI 侧。别同时开着 Claude Code channel 和这个
   bridge —— 两个都会收到同一条消息、都会回复,用户会看到双重回复。
"""

from __future__ import annotations  # 让类型注解不在运行时求值,兼容 Python 3.7+

import collections
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

# ---------------------------------------------------------------------------
# 配置(环境变量;也读同目录 .env)
# ---------------------------------------------------------------------------

def _load_dotenv(path: Path) -> None:
    """极简 .env 加载:KEY=VALUE 逐行;真实环境变量优先。"""
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())
    except FileNotFoundError:
        pass

_load_dotenv(Path(__file__).resolve().parent / ".env")

RELAY_URL = os.environ.get("RELAY_URL", "").rstrip("/")          # 你的域名 + nginx /relay 前缀
SECRET    = os.environ.get("RELAY_SECRET", "")                   # 必须和后端 relay.env 一致
CHAT_ID   = os.environ.get("RELAY_CHAT_ID", "me")               # 单用户通道,固定 "me"
HISTORY_N = int(os.environ.get("HISTORY_N", "12"))             # 喂给模型的最近对话「轮」数
TEMPERATURE = float(os.environ.get("LLM_TEMPERATURE", "0.7"))
HTTP_TIMEOUT = int(os.environ.get("LLM_TIMEOUT", "120"))

# persona = 模型的人设(system prompt)。从 PERSONA 文本或 PERSONA_FILE 文件读。
PERSONA = os.environ.get("PERSONA", "").strip()
_persona_file = os.environ.get("PERSONA_FILE", "").strip()
if not PERSONA and _persona_file:
    try:
        PERSONA = Path(_persona_file).read_text(encoding="utf-8").strip()
    except OSError:
        pass
if not PERSONA:
    PERSONA = "你是对方的 AI 伴侣,在一个私密的一对一聊天里。说话自然、简短、有温度,像在用手机聊天,不要长篇大论。"

# 模型链:主模型 + 可选兜底(LLM_*_2 / _3)。任一返回 FALLBACK_CODES 就顺次切下一个。
def _model_routes() -> list:
    routes = []
    for suffix in ("", "_2", "_3"):
        base = os.environ.get(f"LLM_API_BASE{suffix}", "").rstrip("/")
        key  = os.environ.get(f"LLM_API_KEY{suffix}", "")
        model = os.environ.get(f"LLM_MODEL{suffix}", "")
        if base and model:
            routes.append({"base": base, "key": key, "model": model})
    return routes

MODEL_ROUTES = _model_routes()
FALLBACK_CODES = {401, 403, 404, 408, 409, 429, 500, 502, 503, 504}

# 断线重连游标:只处理 id > cursor 的消息;重连带 ?since=cursor 让 relay 补发。
STATE_DIR = Path(os.environ.get("BRIDGE_STATE_DIR", Path.home() / ".companion-bridge"))
CURSOR_FILE = STATE_DIR / "last_in_id"

# 内存里的滚动对话上下文(避免依赖 relay 历史端点的分页语义 —— 它返回的是「最早」
# 而非「最近」N 条)。收到的人类消息和自己发的回复都 append 进来,喂模型时取尾部。
convo: "collections.deque[dict]" = collections.deque(maxlen=max(HISTORY_N * 2, 8))


def log(tag: str, msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] [{tag}] {msg}", file=sys.stderr, flush=True)


def _require_config() -> None:
    missing = []
    if not RELAY_URL: missing.append("RELAY_URL")
    if not SECRET:    missing.append("RELAY_SECRET")
    if not MODEL_ROUTES: missing.append("LLM_API_BASE + LLM_API_KEY + LLM_MODEL")
    if missing:
        log("fatal", "缺少配置: " + ", ".join(missing) + "  —— 填 .env(见 .env.example)再跑")
        sys.exit(1)


# ---------------------------------------------------------------------------
# relay I/O
# ---------------------------------------------------------------------------

def _auth() -> dict:
    return {"Authorization": f"Bearer {SECRET}"}


def relay_get_json(path: str):
    req = urllib.request.Request(RELAY_URL + path, headers=_auth())
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode("utf-8"))


def relay_post_json(path: str, body: dict):
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        RELAY_URL + path, data=data, method="POST",
        headers={**_auth(), "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        txt = r.read().decode("utf-8")
        return json.loads(txt) if txt else {}


def send_reply(text: str) -> None:
    """AI 的回复 → 落库 + 扇出到 PWA。"""
    out = relay_post_json("/channel/out", {
        "type": "reply", "chat_id": CHAT_ID, "text": text,
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    })
    log("out", f"replied (id={out.get('id')})")


# ---------------------------------------------------------------------------
# 历史 → 内存上下文
# ---------------------------------------------------------------------------

def _row_to_msg(m: dict):
    """把一条 relay 历史/消息转成 OpenAI message;不该进上下文的返回 None。"""
    text = (m.get("text") or "").strip()
    if not text or m.get("kind") == "call":         # 跳过通话开始/结束这类系统事件
        return None
    if m.get("from") == "human":
        return {"role": "user", "content": text}     # 含语音转写(🎤 …)
    if m.get("from") == "ai" and m.get("kind") == "reply":
        return {"role": "assistant", "content": text}  # 跳过 thinking/act 等中间态
    return None


def load_history() -> tuple:
    """翻页拉全部历史 → (近期对话 messages, 最新一条的 id)。relay 的 history 是
    `id > since ASC LIMIT`,所以从 0 往后翻页直到取完,再取尾部当上下文。"""
    rows, since = [], 0
    while True:
        page = relay_get_json(f"/app/history?since={since}&limit=500").get("messages", [])
        if not page:
            break
        rows.extend(page)
        since = page[-1]["id"]
        if len(page) < 500:
            break
    max_id = rows[-1]["id"] if rows else 0
    msgs = [mm for m in rows if (mm := _row_to_msg(m))]
    return msgs[-convo.maxlen:], max_id


def build_messages(custom_persona: str = "", user_persona: str = "") -> list:
    sys_prompt = custom_persona or PERSONA
    if user_persona:
        sys_prompt += f"\n\n[关于与你对话的人类用户的设定]:\n{user_persona}"
    return [{"role": "system", "content": sys_prompt}] + list(convo)


# ---------------------------------------------------------------------------
# 调模型(OpenAI chat/completions;带 fallback 链)
# ---------------------------------------------------------------------------

def _one_call(route: dict, messages: list) -> str:
    body = json.dumps({
        "model": route["model"],
        "messages": messages,
        "temperature": TEMPERATURE,
        # 想接 function calling:在这里加 "tools": [...],处理返回里的 tool_calls,循环喂回(上限 ~8 步)。
    }, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        route["base"] + "/chat/completions", data=body, method="POST",
        headers={"Authorization": f"Bearer {route['key']}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
        data = json.loads(r.read().decode("utf-8"))
    return (data["choices"][0]["message"]["content"] or "").strip()


def call_llm(messages: list) -> str:
    last_err = None
    for route in MODEL_ROUTES:
        try:
            return _one_call(route, messages)
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code in FALLBACK_CODES:
                log("llm", f"{route['model']} HTTP {e.code} → 切下一个")
                continue
            raise
        except (urllib.error.URLError, TimeoutError) as e:
            last_err = e
            log("llm", f"{route['model']} 连接失败({e}) → 切下一个")
            continue
    raise RuntimeError(f"所有模型都失败,最后错误: {last_err}")


# ---------------------------------------------------------------------------
# 一条消息的处理
# ---------------------------------------------------------------------------

def handle_human_message(msg: dict) -> None:
    content = (msg.get("content") or "").strip()
    atts = msg.get("attachments") or []
    if atts:
        names = ", ".join(a.get("name") or "file" for a in atts)
        content = (content + "\n" if content else "") + f"(对方发来 {len(atts)} 个附件: {names})"
    if not content:
        return
    log("in", f"#{msg.get('id')}: {content[:60]}")
    convo.append({"role": "user", "content": content})

    # 读取前端动态附带的 LLM 配置与双人设
    dyn_llm = msg.get("llm_config") or {}
    dyn_personas = msg.get("personas") or {}
    custom_ai = dyn_personas.get("ai") or ""
    custom_user = dyn_personas.get("user") or ""

    active_routes = MODEL_ROUTES
    temp = TEMPERATURE
    if dyn_llm.get("base_url") and dyn_llm.get("model"):
        active_routes = [{
            "base": dyn_llm["base_url"].rstrip("/"),
            "key": dyn_llm.get("api_key") or "",
            "model": dyn_llm["model"]
        }] + MODEL_ROUTES
        if "temperature" in dyn_llm:
            try:
                temp = float(dyn_llm["temperature"])
            except (ValueError, TypeError):
                pass

    try:
        msgs = build_messages(custom_persona=custom_ai, user_persona=custom_user)
        reply = call_llm_dynamic(msgs, active_routes, temp)
    except Exception as e:
        log("err", f"生成失败: {e}")
        return
    if reply:
        convo.append({"role": "assistant", "content": reply})
        send_reply(reply)

def call_llm_dynamic(messages: list, routes: list, temperature: float) -> str:
    last_err = None
    for route in routes:
        try:
            body = json.dumps({
                "model": route["model"],
                "messages": messages,
                "temperature": temperature,
            }, ensure_ascii=False).encode("utf-8")
            headers = {"Content-Type": "application/json"}
            if route.get("key"):
                headers["Authorization"] = f"Bearer {route['key']}"
            req = urllib.request.Request(
                route["base"] + "/chat/completions", data=body, method="POST",
                headers=headers,
            )
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
                data = json.loads(r.read().decode("utf-8"))
            return (data["choices"][0]["message"]["content"] or "").strip()
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code in FALLBACK_CODES:
                log("llm", f"{route['model']} HTTP {e.code} → 切下一个")
                continue
            raise
        except (urllib.error.URLError, TimeoutError) as e:
            last_err = e
            log("llm", f"{route['model']} 连接失败({e}) → 切下一个")
            continue
    raise RuntimeError(f"所有模型都失败,最后错误: {last_err}")

# ---------------------------------------------------------------------------
# SSE 入站流:GET /channel/in(断线自动重连)
# ---------------------------------------------------------------------------

def read_cursor() -> int:
    try:
        return int(CURSOR_FILE.read_text().strip() or "0")
    except (OSError, ValueError):
        return 0


def write_cursor(i: int) -> None:
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        CURSOR_FILE.write_text(str(i))
    except OSError:
        pass


def stream_inbound(cursor: int) -> None:
    backoff = 1
    while True:
        try:
            url = f"{RELAY_URL}/channel/in?since={cursor}"
            req = urllib.request.Request(url, headers={**_auth(), "Accept": "text/event-stream"})
            # timeout 比 relay 的 15s 心跳 ping 长即可:超时=真的断了,跳到重连。
            with urllib.request.urlopen(req, timeout=90) as resp:
                log("in", f"stream connected (since={cursor})")
                backoff = 1
                data_lines: list = []
                for raw in resp:
                    line = raw.decode("utf-8", "replace").rstrip("\r\n")
                    if line.startswith("data:"):
                        data_lines.append(line[5:].lstrip())
                    elif line == "":                      # 空行 = 一帧结束
                        if not data_lines:
                            continue
                        payload, data_lines = "\n".join(data_lines), []
                        try:
                            m = json.loads(payload)
                        except json.JSONDecodeError:
                            continue
                        if m.get("type") == "ping" or "id" not in m:
                            continue
                        mid = int(m.get("id") or 0)
                        if mid <= cursor:                 # 重连补发里已处理过的,跳过
                            continue
                        handle_human_message(m)
                        cursor = mid
                        write_cursor(cursor)              # 只在处理后推进游标
            log("in", "stream ended → reconnect")
        except Exception as e:
            log("in", f"disconnected ({e}) → retry in {backoff}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 15)


def main() -> None:
    _require_config()
    log("boot", f"relay={RELAY_URL}  models={[r['model'] for r in MODEL_ROUTES]}  history={HISTORY_N}")
    cursor = read_cursor()
    # 暖启动:拉历史填上下文,并把全新部署的游标设到「当前最新」——不回放/重答旧消息。
    try:
        ctx, max_id = load_history()
        convo.extend(ctx)
        if cursor == 0:
            cursor = max_id
            write_cursor(cursor)
        log("boot", f"warm-start: {len(convo)} msgs in context, cursor={cursor}")
    except Exception as e:
        log("boot", f"history warm-start skipped ({e})")
    stream_inbound(cursor)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
