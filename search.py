"""
B站视频搜索模块 — WBI 签名 + 搜索 API 封装。

WBI 签名算法与 astrbot_plugin_media_parser 的 bilibili.py 一致，
使用相同的 MIXIN_KEY_ENC_TAB 常量和签名流程，
确保与 B站 API 的兼容性。
"""
import hashlib
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlparse
from pathlib import Path

import aiohttp

# ---------------------------------------------------------------------------
# 常量（与 media_parser bilibili.py 一致）
# ---------------------------------------------------------------------------

MIXIN_KEY_ENC_TAB = [
    46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35,
    27, 43, 5, 49, 33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13,
    37, 48, 7, 16, 24, 55, 40, 61, 26, 17, 0, 1, 60, 51, 30, 4,
    22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11, 36, 20, 34, 44, 52,
]

NAV_API = "https://api.bilibili.com/x/web-interface/nav"
SEARCH_API = "https://api.bilibili.com/x/web-interface/wbi/search/type"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# 排序方式映射
ORDER_MAP: Dict[str, str] = {
    "综合排序": "totalrank",
    "最多播放": "click",
    "最新发布": "pubdate",
    "最多弹幕": "dm",
}
ORDER_LABELS: Dict[str, str] = {v: k for k, v in ORDER_MAP.items()}


# ---------------------------------------------------------------------------
# WBI 签名
# ---------------------------------------------------------------------------

def _extract_key_from_url(url: str) -> str:
    """从 wbi_img URL 中提取 key 片段（文件名不含扩展名）。"""
    try:
        return Path(urlparse(url).path).stem
    except Exception:
        return ""


def _get_mixin_key(img_key: str, sub_key: str) -> str:
    """使用 MIXIN_KEY_ENC_TAB 对 img_key + sub_key 进行置换，取前 32 位。"""
    raw = img_key + sub_key
    return "".join(raw[i] if i < len(raw) else "" for i in MIXIN_KEY_ENC_TAB)[:32]


async def _fetch_wbi_mixin_key(
    session: aiohttp.ClientSession,
    cookie: str = "",
) -> Optional[str]:
    """调用 nav API 获取 wbi_img，计算 mixin_key。

    返回 None 表示获取失败，调用方应回退到无签名模式。
    """
    headers = {
        "User-Agent": UA,
        "Referer": "https://www.bilibili.com",
        "Accept": "application/json, text/plain, */*",
    }
    if cookie:
        headers["Cookie"] = cookie

    try:
        async with session.get(
            NAV_API,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.content_type != "application/json":
                return None
            data = await resp.json()
    except Exception:
        return None

    nav_data = data.get("data") or {}
    if not isinstance(nav_data, dict):
        return None
    wbi_img = nav_data.get("wbi_img") or {}
    if not isinstance(wbi_img, dict):
        return None

    img_url = str(wbi_img.get("img_url", "")).strip()
    sub_url = str(wbi_img.get("sub_url", "")).strip()
    if not img_url or not sub_url:
        return None

    img_key = _extract_key_from_url(img_url)
    sub_key = _extract_key_from_url(sub_url)
    if not img_key or not sub_key:
        return None

    return _get_mixin_key(img_key, sub_key)


def _sign_params(params: Dict[str, Any], mixin_key: str) -> Dict[str, str]:
    """对参数进行 WBI 签名，返回包含 w_rid 和 wts 的新参数字典。"""
    signed: Dict[str, str] = {}
    for k, v in params.items():
        signed[k] = str(v)
    signed["wts"] = str(int(time.time()))

    # 按 key 排序
    ordered = dict(sorted(signed.items(), key=lambda item: item[0]))

    # 过滤特殊字符
    filtered: Dict[str, str] = {}
    remove_chars = "!'()*"
    for key, value in ordered.items():
        text = value
        for ch in remove_chars:
            text = text.replace(ch, "")
        filtered[key] = text

    # 构造 query string 并计算 MD5
    query = urlencode(filtered)
    w_rid = hashlib.md5((query + mixin_key).encode("utf-8")).hexdigest()
    filtered["w_rid"] = w_rid
    return filtered


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

_HTML_RE = re.compile(r"<[^>]+>")

def clean_title(raw: Any) -> str:
    """去除 B站返回文本中的 HTML 标签和实体。"""
    text = str(raw or "")
    text = _HTML_RE.sub("", text)
    text = (
        text.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", '"')
        .replace("&#39;", "'")
    )
    return text.strip()


# ---------------------------------------------------------------------------
# 搜索 API
# ---------------------------------------------------------------------------

async def search_bilibili(
    session: aiohttp.ClientSession,
    keyword: str,
    *,
    cookie: str = "",
    count: int = 3,
    order: str = "totalrank",
    page: int = 1,
) -> Tuple[List[Dict[str, Any]], int]:
    """在 B站搜索视频。

    Args:
        session: aiohttp 会话。
        keyword: 搜索关键词。
        cookie: B站 Cookie 字符串（可选，含 SESSDATA 可提升稳定性）。
        count: 返回结果数量（1-20）。
        order: 排序方式，可选 totalrank / click / pubdate / dm。
        page: 页码（1-50）。

    Returns:
        (视频列表, 总条数) 元组。视频列表每项包含 bvid, aid, title, author, play,
        pic, duration, description, arcurl 字段。总条数最大 1000。

    Raises:
        RuntimeError: API 返回错误时抛出。
    """
    headers = {
        "User-Agent": UA,
        "Referer": "https://www.bilibili.com",
        "Origin": "https://www.bilibili.com",
        "Accept": "application/json, text/plain, */*",
    }
    if cookie:
        headers["Cookie"] = cookie

    # 尝试 WBI 签名；nav 失败则回退到无签名模式
    mixin_key = await _fetch_wbi_mixin_key(session, cookie)
    params: Dict[str, Any] = {
        "search_type": "video",
        "keyword": keyword,
        "order": order,
        "duration": 0,
        "page": page,
    }

    if mixin_key:
        params = _sign_params(params, mixin_key)

    async with session.get(
        SEARCH_API,
        params=params,
        headers=headers,
        timeout=aiohttp.ClientTimeout(total=15),
    ) as resp:
        if resp.content_type != "application/json":
            text = await resp.text()
            raise RuntimeError(f"搜索API返回非JSON响应: {text[:200]}")
        data = await resp.json()

    code = data.get("code")
    if code != 0:
        message = data.get("message", "未知错误")
        raise RuntimeError(f"搜索失败 (code={code}): {message}")

    result_data = data.get("data") or {}
    items = result_data.get("result") or []
    if not isinstance(items, list):
        items = []
    total = result_data.get("numResults", len(items))
    try:
        total = int(total)
    except (TypeError, ValueError):
        total = len(items)

    return items[:count], min(total, 1000)


