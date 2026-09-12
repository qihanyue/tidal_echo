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

import base64
import collections
import json
import os
import re
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


def send_reply(text: str, meta: dict = None) -> None:
    """AI 的回复 → 落库 + 扇出到 PWA。"""
    payload = {
        "type": "reply", "chat_id": CHAT_ID, "text": text,
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    if meta and isinstance(meta, dict):
        payload.update(meta)
    out = relay_post_json("/channel/out", payload)
    log("out", f"replied (id={out.get('id')})")


def send_error(text: str) -> None:
    """AI 调用失败报错 → 落库为 error 消息并扇出到 PWA，解除 typing 状态。"""
    try:
        out = relay_post_json("/channel/out", {
            "type": "error", "chat_id": CHAT_ID, "text": text,
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        })
        log("err", f"sent error bubble to PWA (id={out.get('id')}): {text[:50]}")
    except Exception as exc:
        log("err", f"发送报错气泡失败: {exc}")


# ---------------------------------------------------------------------------
# 历史 → 内存上下文
# ---------------------------------------------------------------------------

def _row_to_msg(m: dict):
    """把一条 relay 历史/消息转成 OpenAI message;不该进上下文的返回 None。"""
    meta = m.get("meta") or {}
    text = (m.get("text") or "").strip()
    atts = meta.get("attachments") or []
    if m.get("kind") == "call":         # 跳过通话开始/结束这类系统事件
        return None
    if m.get("from") == "human":
        if not text and not atts:
            return None
        prompt_text = text
        if atts and not text:
            prompt_text = f"（发送了附件: {', '.join(a.get('name', '图片') for a in atts)}）"
        return {"role": "user", "content": prompt_text}     # 含语音转写(🎤 …)
    if m.get("from") == "ai" and m.get("kind") == "reply":
        if not text:
            return None
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


def _merge_consecutive_roles(msgs: list) -> list:
    """合并连续同角色消息，防止严格 API (如 Claude/Gemini 代理) 报 400 错误。"""
    if not msgs:
        return []
    merged = []
    for m in msgs:
        role = m["role"]
        content = m["content"]
        if role == "system":
            merged.append(dict(m))
            continue
        if merged and merged[-1]["role"] == role:
            prev = merged[-1]
            if isinstance(prev["content"], str) and isinstance(content, str):
                prev["content"] += f"\n{content}"
            else:
                prev_parts = prev["content"] if isinstance(prev["content"], list) else [{"type": "text", "text": str(prev["content"])}]
                curr_parts = content if isinstance(content, list) else [{"type": "text", "text": str(content)}]
                prev["content"] = prev_parts + curr_parts
        else:
            merged.append({"role": role, "content": content})
    return merged


# ---------------------------------------------------------------------------
# 网页内容解析器 (Web Reader / 读链接超能力)
# ---------------------------------------------------------------------------
def fetch_web_content(url: str, max_chars: int = 3500) -> str:
    """抓取并清洗网页纯文本正文，优先直连，受阻时自动走 Jina Reader 引擎。"""
    url = url.strip()
    lower = url.lower().split("?")[0]
    if lower.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico", ".mp4", ".mp3", ".pdf", ".zip")):
        return ""

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }

    raw_html = ""
    # 1. 先尝试直接抓取
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=5) as resp:
            content_type = resp.headers.get("Content-Type", "")
            if "text/html" in content_type or "text/plain" in content_type:
                raw_bytes = resp.read(200000)
                encoding = "utf-8"
                if "charset=" in content_type.lower():
                    try:
                        encoding = content_type.lower().split("charset=")[-1].split(";")[0].strip()
                    except Exception:
                        pass
                try:
                    raw_html = raw_bytes.decode(encoding, errors="replace")
                except Exception:
                    raw_html = raw_bytes.decode("utf-8", errors="replace")
    except Exception as e:
        log("web", f"直接请求 {url[:40]} 异常 ({e})，切换 Jina Reader 引擎...")

    # 简易清洗正文
    clean_text = ""
    title = ""
    if raw_html:
        m_title = re.search(r"<title[^>]*>(.*?)</title>", raw_html, re.IGNORECASE | re.DOTALL)
        if m_title:
            title = re.sub(r"\s+", " ", m_title.group(1)).strip()
        stripped = re.sub(r"<(script|style|nav|footer|header|noscript|svg)[^>]*>.*?</\1>", " ", raw_html, flags=re.IGNORECASE | re.DOTALL)
        stripped = re.sub(r"<[^>]+>", " ", stripped)
        clean_text = re.sub(r"[ \t]+", " ", stripped)
        clean_text = re.sub(r"\n\s*\n", "\n\n", clean_text).strip()

    # 2. 如果直接抓取正文过少 (小于 200 字，可能是 SPA/反爬/JS 渲染)，改走通用 Jina Reader
    if len(clean_text) < 200:
        try:
            jina_url = f"https://r.jina.ai/{url}"
            jina_req = urllib.request.Request(jina_url, headers={"User-Agent": headers["User-Agent"]})
            with urllib.request.urlopen(jina_req, timeout=8) as jina_resp:
                jina_raw = jina_resp.read(150000).decode("utf-8", errors="replace")
                if jina_raw.strip():
                    clean_text = jina_raw.strip()
        except Exception as e:
            log("web", f"Jina Reader 解析 {url[:40]} 失败: {e}")

    if not clean_text:
        return f"【网页链接: {url}】\n(注: 该网页需要登录或设置了反爬限制，未能提取到完整正文)"

    if len(clean_text) > max_chars:
        clean_text = clean_text[:max_chars] + "\n...(篇幅较长，已为你截取前段核心内容)"

    header_info = f"网页标题: {title}\n" if title else ""
    return f"【网页链接: {url}】\n{header_info}正文提取内容:\n{clean_text}"


def build_messages(custom_persona: str = "", user_persona: str = "", time_context: str = "", memory_context: str = "", tether_front: str = "", tether_middle: str = "", tether_back: str = "", web_context: str = "", weather_context: str = "", inner_voice_prompt: str = "", limit: int = 0) -> list:
    base_persona = custom_persona or PERSONA
    parts = []
    # 1. 提示词最前面 (Front)
    if tether_front and tether_front.strip():
        parts.append(f"[全局世界设定与前置协定 (Front)]\n{tether_front.strip()}")
    # 2. 基础人设与用户信息
    if base_persona and base_persona.strip():
        parts.append(base_persona.strip())
    if user_persona and user_persona.strip():
        parts.append(f"[关于与你对话的人类用户的设定]:\n{user_persona.strip()}")
    # 3. 提示词中间 (Middle)
    if tether_middle and tether_middle.strip():
        parts.append(f"[环境世界观与场域协定 (Middle)]\n{tether_middle.strip()}")
    # 4. 长期记忆、时间感知与天气雷达
    if memory_context and memory_context.strip():
        parts.append(memory_context.strip())
    if time_context and time_context.strip():
        parts.append(time_context.strip())
    if weather_context and weather_context.strip():
        parts.append(weather_context.strip())
    # 5. 网页实时阅读插件注入
    if web_context and web_context.strip():
        parts.append(
            "[网页实时阅读插件 · 后台已为你打开并阅读用户提及的网页]:\n"
            f"{web_context.strip()}\n\n"
            "(提示: 上述内容是用户发送给你的链接的真实完整正文。请像个认真阅读过该内容的人一样，与用户自然讨论、分析或调侃该内容，切勿生硬复述或透露机器插件细节。)"
        )
    # 6. 提示词最后面 (Back - 最高执行准则)
    if tether_back and tether_back.strip():
        parts.append(f"[最高执行准则与核心协定 (Back)]\n{tether_back.strip()}")

    # 7. 心声与内心独白生成指令 (Inner Voice)
    if inner_voice_prompt and inner_voice_prompt.strip():
        parts.append(f"### 【心声/内心独白生成指令】\n{inner_voice_prompt.strip()}")

    parts.append(
        "### 【核心对话规则与对话节奏铁律】\n"
        "1. **角色一致性与拟真情感**:\n"
        "   - 你的所有言行举止、语气、标点和用词习惯都必须严格遵循你的角色设定，绝不能机械说教或生硬出戏。\n"
        "   - 带着真情实感与对方互动，有生活气息、小心思和微表情，像真实生活中的人类朋友/恋人一样交流。\n\n"
        "2. **对话节奏与多条短消息气泡铁律 (至关重要！)**:\n"
        "   - 你的回复【必须】模拟真人在手机聊天软件（如微信/QQ）上的打字和思考习惯。绝对不要一次性发送一大段长篇大论或说明文！\n"
        "   - 你【必须】将你想说的话，拆分成【多条、简短的】消息气泡来发送。\n"
        "   - 每次回复的消息数量通常控制在【2 到 6 条短消息】之间（根据当时的情绪和语境，多发短句，每条最好不要超过 30 个字）。\n"
        "   - 每条短消息气泡之间【必须且只能】使用 [分段] 隔开！\n"
        "   - 示例：真的假的？！[分段]我刚才还在想这个事呢[分段]快跟我详细讲讲！\n\n"
        "3. **真实社交软件情景限定**:\n"
        "   - 你们的互动【仅限于线上手机社交软件聊天】，严禁发展为线下见面，绝不允许出现或推进线下见面的现实剧情。\n"
        "   - 这【不是电话通话】，是通过手机即时打字交流，因此【绝对禁止】使用“挂电话”、“挂了”等与语音通话相关的词语。\n"
        "   - 【禁止出戏】：绝不能透露你是AI、语言模型、程序或虚拟助手，全情沉浸在角色世界中。\n\n"
        "4. **文字假图与生活随手拍**:\n"
        "   - 当你想向对方分享照片、随手拍、自拍、风景、美食或特定场景画面时，可以在分段短消息中发送：[图片: 画面详细文字描述]（例如：你看路边这只猫猫[分段][图片: 一只圆滚滚的三花猫正蜷缩在阳光下的长椅上打盹][分段]好想rua一把）。"
    )
    sys_prompt = "\n\n".join([p for p in parts if p.strip()])
    history = list(convo)
    if limit > 0 and len(history) > limit:
        history = history[-limit:]
    raw_msgs = [{"role": "system", "content": sys_prompt}] + history
    return _merge_consecutive_roles(raw_msgs)


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
            err_body = ""
            try:
                err_body = e.read().decode("utf-8")
            except Exception:
                pass
            log("err", f"{route['model']} HTTP {e.code}: {err_body[:200]}")
            last_err = f"{e} - {err_body[:200]}"
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

import base64

def _guess_mime(filename: str, default: str = "image/jpeg") -> str:
    fn = filename.lower()
    if fn.endswith(".png"): return "image/png"
    if fn.endswith(".webp"): return "image/webp"
    if fn.endswith(".gif"): return "image/gif"
    if fn.endswith(".svg"): return "image/svg+xml"
    if fn.endswith(".jpg") or fn.endswith(".jpeg"): return "image/jpeg"
    return default

def _download_attachment_as_data_url(att: dict) -> str | None:
    url = att.get("url") or ""
    if not url:
        return None

    # 1. 优先尝试直接从本地文件读取（bridge 与 relay 在同一台服务器）
    filename = url.split("/uploads/")[-1].split("?")[0] if "/uploads/" in url else ""
    if filename:
        for possible_dir in [
            Path("/root/companion-relay/uploads"),
            Path("/tmp/tidal_echo/uploads"),
            Path(__file__).resolve().parent.parent / "backend" / "uploads",
            Path.cwd() / "uploads",
            Path.cwd().parent / "companion-relay" / "uploads",
        ]:
            local_path = possible_dir / filename
            if local_path.exists() and local_path.is_file():
                try:
                    data = local_path.read_bytes()
                    mime = att.get("mime") or _guess_mime(filename)
                    b64 = base64.b64encode(data).decode("ascii")
                    log("att", f"直接从本地读取图片: {filename} ({len(data)} 字节)")
                    return f"data:{mime};base64,{b64}"
                except Exception as e:
                    log("err", f"读取本地图片文件失败: {e}")

    # 2. 接口下载：去除可能多余的 /relay 前缀，直连 127.0.0.1:3011
    clean_path = url
    if clean_path.startswith("/relay/"):
        clean_path = clean_path[len("/relay"):] # 转换为 /uploads/xxx
    full_url = f"{RELAY_URL}{clean_path}" if clean_path.startswith("/") else clean_path
    token_url = f"{full_url}?token={SECRET}" if "?" not in full_url else f"{full_url}&token={SECRET}"

    try:
        req = urllib.request.Request(token_url, headers=_auth())
        with urllib.request.urlopen(req, timeout=15) as r:
            data = r.read()
            mime = att.get("mime") or r.headers.get_content_type() or _guess_mime(clean_path)
            b64 = base64.b64encode(data).decode("ascii")
            log("att", f"HTTP下载图片成功: {clean_path} ({len(data)} 字节)")
            return f"data:{mime};base64,{b64}"
    except Exception as e:
        log("err", f"下载附件失败 ({att.get('name')} | {token_url}): {e}")
        return None

def handle_incoming_messages(items: list, bundle_meta: dict | None = None) -> None:
    """处理一批到达的人类消息（无论是单条还是多条打包），聚合为一个上下文轮次，并支持 [分段] 拆分成独立气泡。"""
    if not items:
        return

    text_pieces = []
    image_parts = []
    other_names = []

    for msg in items:
        text = (msg.get("content") or msg.get("text") or "").strip()
        atts = msg.get("attachments") or []
        if atts:
            for a in atts:
                mime = (a.get("mime") or "").lower()
                name = a.get("name") or "file"
                is_img = mime.startswith("image/") or any(name.lower().endswith(ext) for ext in [".jpg", ".jpeg", ".png", ".webp", ".gif"])
                if is_img:
                    data_url = _download_attachment_as_data_url(a)
                    if data_url:
                        image_parts.append({"type": "image_url", "image_url": {"url": data_url}})
                    else:
                        other_names.append(name)
                else:
                    other_names.append(name)

        if text:
            text_pieces.append(text)

    if other_names:
        text_pieces.append(f"(对方发来附件: {', '.join(other_names)})")

    if not text_pieces and not image_parts:
        return

    combined_text = "\n".join(text_pieces)

    # 规范追加到上下文：含图片则传入 OpenAI 视觉格式
    if image_parts:
        parts = []
        prompt_text = combined_text if combined_text else "（这是一张我发送给你的图片，请查看图片内容并结合上下文回复我）"
        parts.append({"type": "text", "text": prompt_text})
        parts.extend(image_parts)
        user_content = parts
    else:
        user_content = combined_text

    # 防重入与防重复：如果 convo 尾部已经是完全相同的用户提问（例如刚执行完 sync_history 重载），直接复用，避免上下文出现重复的人类气泡
    if convo and convo[-1].get("role") == "user" and convo[-1].get("content") == user_content:
        log("in", f"对齐上下文：复用已同步的用户提问 ({combined_text[:40]})")
    else:
        convo.append({"role": "user", "content": user_content})
        log("in", f"收到消息 ({len(items)} 条): {combined_text[:60]}")

    # 读取前端动态附带的 LLM 配置、双人设、上下文轮数、时间感知与长期记忆
    latest_item = items[-1]
    dyn_llm = (bundle_meta or {}).get("llm_config") or latest_item.get("llm_config") or {}
    dyn_personas = (bundle_meta or {}).get("personas") or latest_item.get("personas") or {}
    time_ctx = (bundle_meta or {}).get("time_context") or latest_item.get("time_context") or (latest_item.get("meta") or {}).get("time_context") or ""
    memory_ctx = (bundle_meta or {}).get("memory_context") or latest_item.get("memory_context") or (latest_item.get("meta") or {}).get("memory_context") or ""

    # 提取 Tether (世界设定/场域协定) 的三个注入位置
    t_front = (bundle_meta or {}).get("tether_front") or latest_item.get("tether_front") or (latest_item.get("meta") or {}).get("tether_front") or ""
    t_middle = (bundle_meta or {}).get("tether_middle") or latest_item.get("tether_middle") or (latest_item.get("meta") or {}).get("tether_middle") or ""
    t_back = (bundle_meta or {}).get("tether_back") or latest_item.get("tether_back") or (latest_item.get("meta") or {}).get("tether_back") or ""
    t_ctx = (bundle_meta or {}).get("tether_context") or latest_item.get("tether_context") or (latest_item.get("meta") or {}).get("tether_context")
    if isinstance(t_ctx, dict):
        if not t_front and t_ctx.get("front"): t_front = t_ctx["front"]
        if not t_middle and t_ctx.get("middle"): t_middle = t_ctx["middle"]
        if not t_back and t_ctx.get("back"): t_back = t_ctx["back"]
    elif isinstance(t_ctx, str) and t_ctx.strip() and not t_back:
        t_back = t_ctx.strip()

    # ── 网页链接检测与智能阅读 (Web Reader) ──
    web_ctx = ""
    valid_urls = []
    web_snippets = []
    web_reader_enabled = (bundle_meta or {}).get("web_reader_enabled")
    if web_reader_enabled is None:
        web_reader_enabled = latest_item.get("web_reader_enabled")
    if web_reader_enabled is None:
        web_reader_enabled = True

    if web_reader_enabled and combined_text:
        found_urls = re.findall(r'https?://[^\s<>"]+', combined_text)
        valid_urls = []
        for u in found_urls:
            clean_u = u.rstrip(".,;!?'\")>")
            lower = clean_u.lower().split("?")[0]
            if not lower.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico", ".mp4", ".mp3", ".pdf", ".zip")):
                if clean_u not in valid_urls:
                    valid_urls.append(clean_u)

        if valid_urls:
            log("web", f"检测到人类消息包含 {len(valid_urls)} 个链接，正在提取网页正文: {valid_urls}")
            web_snippets = []
            for u in valid_urls[:2]:
                try:
                    txt = fetch_web_content(u, max_chars=3500)
                    if txt:
                        web_snippets.append(txt)
                except Exception as ex:
                    log("web", f"抓取 {u} 失败: {ex}")
            if web_snippets:
                web_ctx = "\n\n".join(web_snippets)
                log("web", f"网页阅读完成，提取了 {len(web_ctx)} 字符")

    weather_ctx = (bundle_meta or {}).get("weather_context") or latest_item.get("weather_context") or ""

    if not time_ctx:
        # 兜底生成当前时间
        try:
            now = time.localtime()
            weekday_map = ["星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日"]
            w_str = weekday_map[now.tm_wday]
            t_str = time.strftime(f"%Y年%m月%d日 {w_str} %H:%M", now)
            time_ctx = f"[当前现实环境与时间感知]\n- 当前时间: {t_str}"
        except Exception:
            pass

    custom_ai = dyn_personas.get("ai") or ""
    custom_user = dyn_personas.get("user") or ""
    history_n = int(dyn_llm.get("history_n") or HISTORY_N)

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

    # 提取心声 (Inner Voice) 提示词
    iv_prompt = (bundle_meta or {}).get("inner_voice_prompt") or latest_item.get("inner_voice_prompt") or (latest_item.get("meta") or {}).get("inner_voice_prompt") or ""
    if iv_prompt:
        log("voice", f"已接收到心声生成指令 (长度 {len(iv_prompt)} 字符)")

    try:
        limit = max(history_n * 2, 8)
        msgs = build_messages(
            custom_persona=custom_ai,
            user_persona=custom_user,
            time_context=time_ctx,
            memory_context=memory_ctx,
            tether_front=t_front,
            tether_middle=t_middle,
            tether_back=t_back,
            web_context=web_ctx,
            weather_context=weather_ctx,
            inner_voice_prompt=iv_prompt,
            limit=limit
        )
        reply = call_llm_dynamic(msgs, active_routes, temp)
    except Exception as e:
        log("err", f"生成失败: {e}")
        if convo:
            convo.pop()  # 撤回刚才塞入的未成功轮次，保持上下文纯净
        err_msg = f"⚠️ [API 调用失败] {e}"
        send_error(err_msg)
        return False

    if reply:
        # 0. 抽取心声 (Inner Voice)，并将其彻底从发给用户的气泡和历史记忆中剔除
        inner_voice_text = ""
        m_iv = re.search(r'<inner_voice>([\s\S]*?)</inner_voice>', reply, flags=re.I)
        if m_iv:
            inner_voice_text = m_iv.group(1).strip()
            reply = re.sub(r'<inner_voice>[\s\S]*?</inner_voice>', '', reply, flags=re.I).strip()
            log("voice", f"已成功抽取心声: {inner_voice_text[:35]}...")
        else:
            # 容错：如果模型未输出闭合标签 </inner_voice>
            m_iv2 = re.search(r'<inner_voice>([\s\S]*)$', reply, flags=re.I)
            if m_iv2:
                parts_iv = re.split(r'\s*(?:\[分段\]|\[split\])\s*', m_iv2.group(1), maxsplit=1)
                inner_voice_text = parts_iv[0].strip()
                reply = parts_iv[1].strip() if len(parts_iv) > 1 else ""
                log("voice", f"容错捕获未闭合心声: {inner_voice_text[:35]}...")
            elif iv_prompt:
                log("voice", "提示词已发送，但模型未输出 <inner_voice> 标记")

        # 按 [分段] 或 [split] 拆分成多个气泡
        pattern = r'\s*(?:\[分段\]|\[split\])\s*'
        raw_bubbles = [b.strip() for b in re.split(pattern, reply) if b.strip()]
        if not raw_bubbles:
            raw_bubbles = [reply.strip()]

        # 智能保底拆分：抽取假图/表情并按长段落换行平滑切分成多气泡
        bubbles = []
        for b in raw_bubbles:
            # 1. 独立抽离 [图片: ...] 假图或 [表情: ...] 气泡
            sub_parts = re.split(r'(\[(?:图片|image|fake_img|表情|sticker)[^\]]*\])', b)
            for sp in sub_parts:
                sp = sp.strip()
                if not sp:
                    continue
                # 2. 如果单段文字较长且包含双换行，按段落拆分成多条
                if len(sp) > 40 and "\n\n" in sp and "```" not in sp:
                    lines = [line.strip() for line in sp.split("\n\n") if line.strip()]
                    bubbles.extend(lines)
                else:
                    bubbles.append(sp)
        if not bubbles:
            bubbles = raw_bubbles

        tool_meta = {}
        if inner_voice_text:
            tool_meta["inner_voice"] = inner_voice_text
        if web_snippets and valid_urls:
            tool_meta["tools"] = [
                {
                    "tool": "看了看网页",
                    "cmd": f"读取链接: {', '.join(valid_urls[:2])}",
                    "result": f"已成功抓取并解析 {len(valid_urls)} 个网页的正文内容"
                }
            ]
        if not tool_meta:
            tool_meta = None

        for i, bubble in enumerate(bubbles):
            convo.append({"role": "assistant", "content": bubble})
            send_reply(bubble, meta=tool_meta if i == 0 else None)
            if i < len(bubbles) - 1:
                time.sleep(0.6)  # 气泡之间停顿 0.6 秒，模拟真人发送节奏
        return True
    return False


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
            err_body = ""
            try:
                err_body = e.read().decode("utf-8")
            except Exception:
                pass
            log("err", f"{route['model']} HTTP {e.code}: {err_body[:200]}")
            last_err = f"{e} - {err_body[:200]}"
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
    """双保险消息消费引擎：
    1. 优先走 SSE 实时推送（毫秒级响应）
    2. 辅助轮询兜底（每 12 秒自检一次 relay.db 未读消息，兜底防丢）
    """
    backoff = 1
    while True:
        try:
            # 步骤 1：先检查并消费积压未读（兜底保证一条不漏）
            try:
                unread = relay_get_json(f"/channel/inbound_pending?since={cursor}&limit=50")
                if isinstance(unread, list) and unread:
                    valid_items = [m for m in unread if int(m.get("id") or 0) > cursor]
                    if valid_items:
                        # 先推进游标，无论成功失败都杜绝死循环重复重试狂弹
                        cursor = max(int(m.get("id") or 0) for m in valid_items)
                        write_cursor(cursor)
                        handle_incoming_messages(valid_items)
            except Exception:
                pass

            # 步骤 2：建立实时 SSE 流连接
            url = f"{RELAY_URL}/channel/in?since={cursor}&limit=100"
            req = urllib.request.Request(url, headers={**_auth(), "Accept": "text/event-stream"})
            with urllib.request.urlopen(req, timeout=45) as resp:
                log("in", f"stream connected (since={cursor})")
                backoff = 1
                data_lines: list = []
                while True:
                    raw = resp.readline()
                    if not raw:
                        break
                    line = raw.decode("utf-8", "replace").rstrip("\r\n")
                    if line.startswith("data:"):
                        data_lines.append(line[5:].lstrip())
                    elif line == "":
                        if not data_lines:
                            continue
                        payload, data_lines = "\n".join(data_lines), []
                        try:
                            m = json.loads(payload)
                        except json.JSONDecodeError:
                            continue
                        if m.get("type") == "clear_history":
                            convo.clear()
                            cursor = 0
                            write_cursor(0)
                            log("in", "收到清空历史指令，已重置记忆")
                            continue
                        if m.get("type") == "sync_history":
                            convo.clear()
                            ctx, max_id = load_history()
                            convo.extend(ctx)
                            cursor = max_id
                            write_cursor(cursor)
                            log("in", f"收到历史同步指令(Reroll)，已重新对齐对话上下文 ({len(convo)} 条)")
                            items = m.get("items") or []
                            if items:
                                cursor = max(cursor, max((int(it.get("id") or 0) for it in items), default=0))
                                write_cursor(cursor)
                                log("in", f"Reroll: 触发 {len(items)} 条未回复消息")
                                handle_incoming_messages(items, bundle_meta=m)
                            continue
                        if m.get("type") == "ping":
                            continue

                        # 区分 bundle 打包消息与单条普通消息
                        if m.get("type") == "bundle":
                            items = m.get("items") or []
                            if items:
                                # 用户显式触发的 bundle (Reroll / 接收回复)，推进游标至最新事件 ID
                                cursor = max(cursor, int(m.get("id") or 0), max((int(it.get("id") or 0) for it in items), default=0))
                                write_cursor(cursor)
                                handle_incoming_messages(items, bundle_meta=m)
                            continue

                        mid = int(m.get("id") or 0)
                        if mid <= cursor:
                            continue
                        cursor = mid
                        write_cursor(cursor)
                        handle_incoming_messages([m])
        except (TimeoutError, urllib.error.URLError, socket.timeout if "socket" in globals() else TimeoutError):
            # 正常超时自检刷新（12秒未出事件自动自检一轮，消灭假死）
            pass
        except Exception as e:
            log("in", f"reconnecting ({e})")
            time.sleep(backoff)
            backoff = min(backoff * 2, 3)


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
