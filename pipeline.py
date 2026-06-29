"""视频下载+发送流水线，封装 media_parser 集成。"""
import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

logger = logging.getLogger(__name__)

# 导入 media_parser 工具
_register_files_with_token_service = None
try:
    from astrbot_plugin_media_parser.core.storage import (
        register_files_with_token_service as _register_files_with_token_service,
    )
except ImportError:
    import os as _os
    import sys as _sys
    _parent = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
    if _parent not in _sys.path:
        _sys.path.insert(0, _parent)
    try:
        from astrbot_plugin_media_parser.core.storage import (
            register_files_with_token_service as _register_files_with_token_service,
        )
    except ImportError:
        pass


def strip_range(metadata: Dict[str, Any]) -> None:
    """去 metadata 中 video_urls 的 'range:' 前缀，避免并发分片触发B站限流。"""
    raw = metadata.get("video_urls") or []
    stripped = []
    for g in raw:
        if isinstance(g, list):
            stripped.append([
                u[6:] if isinstance(u, str) and u.startswith("range:") else u
                for u in g
            ])
        elif isinstance(g, str) and g.startswith("range:"):
            stripped.append(g[6:])
        else:
            stripped.append(g)
    metadata["video_urls"] = stripped


async def parse_and_download_batch(
    mp,  # VideoParserPlugin 实例
    session: aiohttp.ClientSession,
    video_urls: List[str],
    bilibili_cookie: str = "",
    proxy_addr: str = "",
) -> List[Dict[str, Any]]:
    """批量解析+下载视频 URL，返回含 file_paths 的 metadata 列表。"""
    url_text = "\n".join(video_urls)

    # 提取链接
    links_with_parser = mp.parser_manager.extract_all_links(url_text)
    if not links_with_parser:
        logger.error("[girlvideo] media_parser 未能识别任何链接")
        return []

    # 解析
    metadata_list = await mp.parser_manager.parse_text(
        url_text, session, links_with_parser=links_with_parser
    )
    if not metadata_list:
        logger.error("[girlvideo] 所有视频解析失败")
        return []

    logger.info(f"[girlvideo] 视频解析完成: {len(metadata_list)}/{len(video_urls)}")

    # 下载
    processed: List[Dict[str, Any]] = []
    for metadata in metadata_list:
        if metadata.get("error"):
            continue
        strip_range(metadata)

        async def _do_download(meta):
            pm = await mp.download_manager.process_metadata(
                session, meta, proxy_addr=proxy_addr or None,
            )
            fps = pm.get("file_paths") or []
            vms = pm.get("video_modes") or []
            ok = any(fp and vm == "local" for fp, vm in zip(fps, vms))
            return pm, ok

        try:
            pm, ok = await _do_download(metadata)
            if not ok:
                url = metadata.get("url", "")
                logger.info(f"[girlvideo] 下载失败，重新解析: {url[:60]}")
                try:
                    fresh = await mp.bilibili_parser.parse_bilibili_minimal(
                        url, session=session,
                        cookie_header_override=bilibili_cookie or None,
                    )
                    if fresh:
                        strip_range(fresh)
                        pm, ok = await _do_download(fresh)
                except Exception:
                    pass
            processed.append(pm)
        except Exception as e:
            logger.error(f"[girlvideo] 下载异常: {type(e).__name__}: {e}")

    if processed:
        logger.info(f"[girlvideo] 视频下载完成: {len(processed)}/{len(metadata_list)}")
    else:
        logger.error("[girlvideo] 所有视频下载失败")

    return processed


async def parse_and_download_one(
    mp,
    session: aiohttp.ClientSession,
    video_url: str,
    bilibili_cookie: str = "",
    proxy_addr: str = "",
    max_size_mb: int = 0,
    timeout: float = 90,
) -> Tuple[Optional[Dict[str, Any]], bool]:
    """解析+下载单个视频。返回 (metadata, success)。

    超时或失败时返回 (None, False)。
    """
    try:
        metadata = await asyncio.wait_for(
            mp.bilibili_parser.parse_bilibili_minimal(
                video_url, session=session,
                cookie_header_override=bilibili_cookie or None,
            ),
            timeout=timeout,
        )
    except (asyncio.TimeoutError, Exception):
        return None, False

    if not metadata:
        return None, False

    strip_range(metadata)

    # 大小预检
    if max_size_mb > 0:
        first_url = ""
        for g in (metadata.get("video_urls") or []):
            if isinstance(g, list) and g:
                first_url = g[0]
            elif isinstance(g, str) and g:
                first_url = g
            if first_url:
                break
        if first_url and not first_url.startswith(("dash:", "m3u8:")):
            try:
                async with session.head(
                    first_url, timeout=aiohttp.ClientTimeout(total=10),
                ) as hr:
                    cl = hr.headers.get("Content-Length")
                    if cl:
                        size_mb = int(cl) / (1024 * 1024)
                        if size_mb > max_size_mb:
                            logger.info(
                                f"[girlvideo] 视频过大 {size_mb:.0f}MB"
                                f" > {max_size_mb}MB，跳过"
                            )
                            return None, False
            except Exception:
                pass

    try:
        pm = await asyncio.wait_for(
            mp.download_manager.process_metadata(
                session, metadata, proxy_addr=proxy_addr or None,
            ),
            timeout=timeout,
        )
    except (asyncio.TimeoutError, Exception):
        return None, False

    fps = pm.get("file_paths") or []
    vms = pm.get("video_modes") or []
    ok = any(fp and vm == "local" for fp, vm in zip(fps, vms))
    return pm, ok


def build_video_chain(
    meta: Dict[str, Any],
) -> List[Tuple[Any, bool]]:
    """从 metadata 构建视频消息链。

    返回 [(component, use_relay), ...] 列表。
    """
    from astrbot.api.message_components import Video as CompVideo

    use_relay = bool(meta.get("use_file_token_service"))
    urls = (
        meta.get("file_token_urls")
        if use_relay
        else meta.get("file_paths")
    )
    result = []
    for url in (urls or []):
        if not url:
            continue
        if use_relay:
            result.append((CompVideo.fromURL(url), True))
        else:
            result.append((CompVideo.fromFileSystem(url), False))
    return result


async def register_relay(
    processed: List[Dict[str, Any]],
    relay_callback_url: str,
    relay_ttl: int,
) -> None:
    """批量注册文件到 AstrBot 中转服务。"""
    if not _register_files_with_token_service:
        logger.warning("[girlvideo] 中转模块未加载")
        return

    for meta in processed:
        try:
            await _register_files_with_token_service(
                meta, relay_callback_url, relay_ttl,
            )
        except Exception as e:
            logger.error(f"[girlvideo] 中转注册异常: {e}")
