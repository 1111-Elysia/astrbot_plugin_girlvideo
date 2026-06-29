"""
astrbot_plugin_girlvideo - B站视频搜索插件

触发方式: 消息中包含配置的触发关键词时自动搜索B站视频并下载发送。
"""
import asyncio
import base64
import os
import re
import shutil
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

import aiohttp

from astrbot.api import logger
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.message_components import Video as CompVideo
from astrbot.api.star import Context, Star, register
from astrbot.api.web import json_response, request

from .search import search_bilibili
from .storage import SentRecordStore, OriginStore, get_data_dir
from .scheduler import Scheduler

# ---------------------------------------------------------------------------
# 导入 media_parser 工具函数
# ---------------------------------------------------------------------------
_register_relay = None
try:
    from astrbot_plugin_media_parser.core.storage import (
        register_files_with_token_service as _register_relay,
    )
except ImportError:
    import sys as _sys, os as _os
    _parent = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
    if _parent not in _sys.path:
        _sys.path.insert(0, _parent)
    try:
        from astrbot_plugin_media_parser.core.storage import (
            register_files_with_token_service as _register_relay,
        )
    except ImportError:
        pass


# ---------------------------------------------------------------------------
# 插件主类
# ---------------------------------------------------------------------------

@register(
    "astrbot_plugin_girlvideo",
    "pgd",
    "B站视频搜索下载：通过关键词在B站搜索视频并下载发送",
    "1.0.0",
)
class GirlVideoPlugin(Star):

    def __init__(self, context: Context, config: dict) -> None:
        super().__init__(context)

        # ── 触发配置 ──
        trigger_cfg = config.get("trigger") or {}
        raw_keywords = trigger_cfg.get("keywords") or ["搜视频"]
        if isinstance(raw_keywords, str):
            raw_keywords = [
                kw.strip() for kw in raw_keywords.split(",") if kw.strip()
            ]
        if not raw_keywords:
            raw_keywords = ["搜视频"]
        self.trigger_keywords: List[str] = [
            str(kw).strip() for kw in raw_keywords if str(kw).strip()
        ]

        # ── 中转配置 ──
        relay_cfg = config.get("relay") or {}
        self.relay_enable = bool(relay_cfg.get("enable", True))
        self.relay_callback_url = str(
            relay_cfg.get("callback_url", "") or ""
        ).strip().rstrip("/")
        try:
            self.relay_ttl = max(60, min(86400, int(relay_cfg.get("ttl", 300))))
        except (TypeError, ValueError):
            self.relay_ttl = 300

        # ── 持久化 ──
        self._data_dir = get_data_dir()
        self._sent_store = SentRecordStore(self._data_dir)
        self._origin_store = OriginStore(self._data_dir)

        # ── B站配置 ──
        bili_cfg = config.get("bilibili") or {}
        self.bilibili_cookie = str(bili_cfg.get("cookie", "") or "").strip()

        # ── 定时任务 ──
        self._scheduler = Scheduler(
            context=context,
            sent_store=self._sent_store,
            origin_store=self._origin_store,
            timer_tasks=config.get("timer_tasks") or [],
            bilibili_cookie=self.bilibili_cookie,
            relay_enable=self.relay_enable,
            relay_callback_url=self.relay_callback_url,
            relay_ttl=self.relay_ttl,
        )
        self._scheduler.start()

        # ── 注册 WebUI 页面 API ──
        context.register_web_api(
            "/astrbot_plugin_girlvideo/search",
            self._api_search, ["POST"], "测试搜索"
        )
        context.register_web_api(
            "/astrbot_plugin_girlvideo/sent-bvids",
            self._api_sent_bvids, ["GET"], "已发送BV号"
        )
        context.register_web_api(
            "/astrbot_plugin_girlvideo/config",
            self._api_config, ["GET"], "插件配置"
        )
        context.register_web_api(
            "/astrbot_plugin_girlvideo/proxy-image",
            self._api_proxy_image, ["GET"], "图片代理"
        )
        context.register_web_api(
            "/astrbot_plugin_girlvideo/cover-base64",
            self._api_cover_base64, ["GET"], "封面base64"
        )

        # ── 缓存清理 ──
        cache_cfg = config.get("cache") or {}
        try:
            self._cache_cleanup_interval = max(
                0, int(cache_cfg.get("cleanup_interval_minutes", 60))
            )
        except (TypeError, ValueError):
            self._cache_cleanup_interval = 60
        self._cache_cleanup_task: Optional[asyncio.Task] = None
        if self._cache_cleanup_interval > 0:
            self._cache_cleanup_task = asyncio.create_task(
                self._cache_cleanup_loop()
            )

        logger.info(
            f"[girlvideo] 插件初始化完成 "
            f"(触发词: {self.trigger_keywords}, "
            f"Cookie: {'已配置' if self.bilibili_cookie else '未配置'}, "
            f"中转: {'启用' if self.relay_enable else '关闭'}"
            f"{'(' + self.relay_callback_url + ')' if self.relay_enable and self.relay_callback_url else ''})"
        )

    # ── 主事件处理 ────────────────────────────────────────

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_message(self, event: AstrMessageEvent):
        message_str = event.message_str.strip()
        if not message_str:
            return

        # 缓存群聊 origin
        group_id = event.get_group_id()
        if group_id:
            umo = getattr(event, "unified_msg_origin", "")
            if umo and self._origin_store.set(str(group_id), umo):
                logger.info(
                    f"[girlvideo] 缓存 origin | "
                    f"group={group_id} origin={umo}"
                )

        # 检查触发关键词
        matched = self._match_trigger(message_str)
        if not matched:
            return

        sender_name = event.get_sender_name()
        sender_id = event.get_sender_id()
        logger.info(f"[girlvideo] 触发 | 用户: {sender_name}({sender_id})")

        # 提取搜索关键词
        keyword, count = self._extract_search_params(message_str, matched)
        if not keyword:
            logger.info("[girlvideo] 未提供搜索关键词，跳过")
            return
        if count <= 0:
            count = 1
        count = min(count, 5)

        logger.info(f"[girlvideo] 开始搜索 | 数量: {count}")

        # 获取 media_parser
        mp = self._get_media_parser()
        if mp is None:
            logger.error("[girlvideo] media_parser 未加载")
            return

        _proxy_addr = ""
        try:
            _proxy_addr = str(getattr(mp.cfg.proxy, "address", "") or "").strip()
        except Exception:
            pass

        # 搜索
        sent_bvids = self._sent_store.load()
        results: List[Dict[str, Any]] = []
        words = keyword.split()

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30)
        ) as session:
            current_words = list(words)
            while current_words:
                search_kw = " ".join(current_words)
                pg = 1
                while pg <= 5:
                    try:
                        page_results, _ = await search_bilibili(
                            session, keyword=search_kw,
                            cookie=self.bilibili_cookie,
                            count=20, order="totalrank", page=pg,
                        )
                    except RuntimeError as e:
                        logger.error(f"[girlvideo] 搜索失败(第{pg}页): {e}")
                        page_results = []
                    if not page_results:
                        break
                    fresh = [
                        item for item in (page_results or [])
                        if str(item.get("bvid", "")).strip() not in sent_bvids
                    ]
                    if fresh:
                        results = fresh
                        break
                    pg += 1
                if results:
                    break
                if len(current_words) <= 1:
                    break
                import random
                drop = random.choice(current_words)
                current_words = [w for w in current_words if w != drop]

        results = results[:count]
        if not results:
            logger.info("[girlvideo] 无未发送视频")
            return

        logger.info(f"[girlvideo] 搜索完成 | 最终结果: {len(results)} 条")

        # 构造 URL
        video_urls = [
            f"https://www.bilibili.com/video/{str(item.get('bvid', '')).strip()}"
            for item in results
            if str(item.get("bvid", "")).strip()
        ]
        if not video_urls:
            return

        url_text = "\n".join(video_urls)

        # 解析 + 下载
        timeout_dl = aiohttp.ClientTimeout(total=300)
        async with aiohttp.ClientSession(timeout=timeout_dl) as session:
            links = mp.parser_manager.extract_all_links(url_text)
            if not links:
                return

            metadata_list = await mp.parser_manager.parse_text(
                url_text, session, links_with_parser=links
            )
            if not metadata_list:
                return

            processed: List[Dict[str, Any]] = []
            for metadata in metadata_list:
                if metadata.get("error"):
                    continue
                # 去 range:
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

                async def _dl(meta):
                    pm = await mp.download_manager.process_metadata(
                        session, meta, proxy_addr=_proxy_addr or None,
                    )
                    fps = pm.get("file_paths") or []
                    vms = pm.get("video_modes") or []
                    ok = any(fp and vm == "local" for fp, vm in zip(fps, vms))
                    return pm, ok

                try:
                    pm, ok = await _dl(metadata)
                    if not ok:
                        url = metadata.get("url", "")
                        try:
                            fresh = await mp.bilibili_parser.parse_bilibili_minimal(
                                url, session=session,
                                cookie_header_override=self.bilibili_cookie or None,
                            )
                            if fresh:
                                raw2 = fresh.get("video_urls") or []
                                s2 = []
                                for g in raw2:
                                    if isinstance(g, list):
                                        s2.append([
                                            u[6:] if isinstance(u, str) and u.startswith("range:") else u
                                            for u in g
                                        ])
                                    elif isinstance(g, str) and g.startswith("range:"):
                                        s2.append(g[6:])
                                    else:
                                        s2.append(g)
                                fresh["video_urls"] = s2
                                pm, ok = await _dl(fresh)
                        except Exception:
                            pass
                    processed.append(pm)
                except Exception:
                    pass

            if not processed:
                return

            # 中转
            if (
                self.relay_enable
                and self.relay_callback_url
                and _register_relay
            ):
                for meta in processed:
                    try:
                        await _register_relay(
                            meta, self.relay_callback_url, self.relay_ttl,
                        )
                    except Exception:
                        pass

            # 发送纯视频
            sent_count = 0
            for meta in processed:
                use_relay = bool(meta.get("use_file_token_service"))
                urls = (
                    meta.get("file_token_urls")
                    if use_relay
                    else meta.get("file_paths")
                )
                for url in (urls or []):
                    if not url:
                        continue
                    if use_relay:
                        yield event.chain_result([CompVideo.fromURL(url)])
                    else:
                        yield event.chain_result([CompVideo.fromFileSystem(url)])
                    sent_count += 1

            if sent_count > 0:
                for item in results:
                    bvid = str(item.get("bvid", "")).strip()
                    if bvid:
                        self._sent_store.mark(bvid)

            logger.info(
                f"[girlvideo] 视频发送完成 | "
                f"用户: {sender_name}({sender_id}) | "
                f"视频数: {sent_count}"
            )

    # ── 辅助方法 ──────────────────────────────────────────

    def _get_media_parser(self):
        try:
            plugins = self.context.get_all_stars()
        except Exception as e:
            logger.error(f"[girlvideo] 获取插件列表失败: {e}")
            return None
        for meta in plugins:
            if (
                getattr(meta, "name", None) == "astrbot_plugin_media_parser"
                and getattr(meta, "activated", False)
                and getattr(meta, "star_cls", None) is not None
            ):
                return meta.star_cls
        return None

    def _match_trigger(self, msg: str) -> Optional[str]:
        for kw in self.trigger_keywords:
            if msg.startswith(kw) or kw in msg:
                return kw
        return None

    def _extract_search_params(self, msg: str, matched: str) -> Tuple[str, int]:
        idx = msg.find(matched)
        text = msg[idx + len(matched):].strip()
        count = -1
        m = re.search(r"--数量\s+(\d+)", text)
        if m:
            try:
                count = int(m.group(1))
            except ValueError:
                count = -1
            text = re.sub(r"--数量\s+\d+", "", text).strip()
        return text.strip(), count

    # ── WebUI API ──────────────────────────────────────────

    async def _api_config(self):
        """返回当前插件配置（供页面使用）。"""
        timer_tasks = []
        for t in self._scheduler._timer_tasks:
            if t.get("enabled") and t.get("keyword"):
                timer_tasks.append({
                    "keyword": t.get("keyword", ""),
                    "interval_minutes": t.get("interval_minutes", 60),
                    "count": t.get("count", 1),
                })
        return json_response({"timer_tasks": timer_tasks})

    async def _api_sent_bvids(self):
        """返回已发送的 BV 号列表。"""
        bvids = sorted(self._sent_store.load())
        return json_response({"bvids": bvids, "count": len(bvids)})

    async def _api_search(self):
        """执行测试搜索并返回结果。"""
        payload = await request.json(default={})
        keyword = str(payload.get("keyword", "")).strip()
        order = str(payload.get("order", "totalrank")).strip()
        if not keyword:
            return json_response({"error": "请提供关键词"}, status_code=400)

        sent_bvids = self._sent_store.load()
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15)
        ) as session:
            try:
                raw, total = await search_bilibili(
                    session, keyword=keyword,
                    cookie=self.bilibili_cookie,
                    count=20, order=order, page=1,
                )
            except RuntimeError as e:
                return json_response({"error": str(e)})

        results = []
        for item in (raw or []):
            bvid = str(item.get("bvid", "")).strip()
            results.append({
                "title": str(item.get("title", "")),
                "author": str(item.get("author", "")),
                "bvid": bvid,
                "aid": item.get("aid"),
                "play": item.get("play", 0),
                "duration": str(item.get("duration", "")),
                "pic": str(item.get("pic", "")),
                "sent": bvid in sent_bvids if bvid else False,
            })

        fresh = sum(1 for r in results if r["bvid"] and not r["sent"])
        return json_response({
            "keyword": keyword,
            "total": total,
            "page_total": len(results),
            "fresh": fresh,
            "results": results,
        })

    async def _api_proxy_image(self):
        """代理 B站封面图，添加 Referer 绕过防盗链。"""
        url = request.query.get("url", "").strip()
        if not url:
            return json_response({"error": "缺少url参数"}, status_code=400)

        # 规范化 URL: 协议相对 → https
        if url.startswith("//"):
            url = "https:" + url
        elif url.startswith("http://"):
            url = url.replace("http://", "https://", 1)

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Referer": "https://www.bilibili.com",
        }
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(
                    url, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status != 200:
                        return json_response(
                            {"error": f"上游返回 {resp.status}"},
                            status_code=502,
                        )
                    data = await resp.read()
                    ct = resp.headers.get("Content-Type", "image/jpeg")
                    # 返回原始字节（Quart 兼容）
                    from quart import Response
                    return Response(data, mimetype=ct)
            except Exception as e:
                return json_response({"error": str(e)}, status_code=502)

    async def _api_cover_base64(self):
        """获取封面图的 base64，供页面通过 bridge 调用（绕过沙箱限制）。"""
        url = request.query.get("url", "").strip()
        if not url:
            return json_response({"error": "缺少url参数"}, status_code=400)
        if url.startswith("//"):
            url = "https:" + url
        elif url.startswith("http://"):
            url = url.replace("http://", "https://", 1)

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.bilibili.com",
        }
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(
                    url, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status != 200:
                        return json_response({"error": f"上游返回 {resp.status}"})
                    data = await resp.read()
                    return json_response({
                        "base64": base64.b64encode(data).decode(),
                        "size": len(data),
                    })
            except Exception as e:
                return json_response({"error": str(e)})

    # ── 缓存清理 ──────────────────────────────────────────

    async def _cache_cleanup_loop(self) -> None:
        """定期清理 media_parser 视频缓存目录。"""
        interval_s = self._cache_cleanup_interval * 60
        logger.info(
            f"[girlvideo] 缓存清理已启动 | 间隔: {self._cache_cleanup_interval}min"
        )
        while True:
            await asyncio.sleep(interval_s)
            try:
                mp = self._get_media_parser()
                if mp is None:
                    continue
                cache_dir = mp.download_manager.cache_dir
                if not cache_dir or not os.path.isdir(cache_dir):
                    continue
                # 清理缓存目录下所有文件/子目录
                count = 0
                for entry in os.listdir(cache_dir):
                    path = os.path.join(cache_dir, entry)
                    try:
                        if os.path.isfile(path):
                            os.remove(path)
                            count += 1
                        elif os.path.isdir(path):
                            shutil.rmtree(path, ignore_errors=True)
                            count += 1
                    except Exception:
                        pass
                if count > 0:
                    logger.info(
                        f"[girlvideo] 缓存清理完成 | "
                        f"删除 {count} 项 | 目录: {cache_dir}"
                    )
            except Exception as e:
                logger.error(f"[girlvideo] 缓存清理异常: {e}")

    # ── 生命周期 ──────────────────────────────────────────

    async def terminate(self) -> None:
        await self._scheduler.stop()
        if self._cache_cleanup_task:
            self._cache_cleanup_task.cancel()
