"""定时任务调度器：定时触发搜索+下载+发送。"""
import asyncio
import random
import time
from typing import Any, Dict, List, Optional, Set, Tuple

import aiohttp

from astrbot.api import logger
from astrbot.api.event import MessageChain
from astrbot.api.message_components import Video as CompVideo
from astrbot.api.star import Star

from .search import search_bilibili, ORDER_MAP
from .storage import SentRecordStore, OriginStore


def _parse_duration(dur: str) -> int:
    """解析 B站时长格式为秒数：3:45→225, 1:23:45→5025。"""
    if not dur or not dur.strip():
        return 0
    parts = dur.strip().split(":")
    try:
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        elif len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        return int(parts[0])
    except (ValueError, TypeError):
        return 0


def _get_media_parser(context) -> Optional[Star]:
    """获取 media_parser 实例。"""
    try:
        plugins = context.get_all_stars()
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


class Scheduler:
    """定时任务调度器。"""

    def __init__(
        self,
        context,
        sent_store: SentRecordStore,
        origin_store: OriginStore,
        timer_tasks: List[Dict[str, Any]],
        bilibili_cookie: str = "",
        relay_enable: bool = False,
        relay_callback_url: str = "",
        relay_ttl: int = 300,
    ):
        self._context = context
        self._sent_store = sent_store
        self._origin_store = origin_store
        self._timer_tasks = timer_tasks
        self._bilibili_cookie = bilibili_cookie
        self._relay_enable = relay_enable
        self._relay_callback_url = relay_callback_url
        self._relay_ttl = relay_ttl

        self._task_last_run: Dict[int, float] = {}
        self._task_running = False
        self._active_tasks: Set[asyncio.Task] = set()
        self._loop_task: Optional[asyncio.Task] = None

    def start(self) -> None:
        enabled = [
            t for t in self._timer_tasks
            if t.get("enabled") and t.get("keyword")
        ]
        if enabled:
            self._loop_task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        if self._loop_task:
            self._loop_task.cancel()
        for t in list(self._active_tasks):
            t.cancel()
        self._active_tasks.clear()
        logger.info("[girlvideo] 调度器已停止")

    async def _loop(self) -> None:
        logger.info("[girlvideo] 调度器已启动")
        while True:
            try:
                await asyncio.sleep(30)
                now = time.time()
                for idx, task in enumerate(self._timer_tasks):
                    if not task.get("enabled"):
                        continue
                    keyword = str(task.get("keyword", "")).strip()
                    if not keyword:
                        continue
                    try:
                        interval = max(1, int(task.get("interval_minutes", 60)))
                    except (TypeError, ValueError):
                        interval = 60
                    if now - self._task_last_run.get(idx, 0) >= interval * 60:
                        if self._task_running:
                            logger.info("[girlvideo] 上一任务未完成，跳过本轮")
                            continue
                        logger.info(f"[girlvideo] 定时任务触发 | 间隔: {interval}min")
                        self._task_last_run[idx] = now
                        self._task_running = True
                        t = asyncio.create_task(self._execute(task))
                        self._active_tasks.add(t)
                        t.add_done_callback(
                            lambda _t: (
                                self._active_tasks.discard(_t),
                                setattr(self, '_task_running', False),
                            )
                        )
            except Exception as e:
                logger.error(f"[girlvideo] 调度器异常: {e}")
                await asyncio.sleep(60)

    async def _execute(self, task: Dict[str, Any]) -> None:
        try:
            await self._execute_impl(task)
        except Exception as e:
            logger.error(
                f"[girlvideo] 定时任务崩溃: {type(e).__name__}: {e}",
                exc_info=True,
            )

    async def _execute_impl(self, task: Dict[str, Any]) -> None:
        keyword = str(task.get("keyword", "")).strip()
        try:
            count = max(1, min(5, int(task.get("count", 1))))
        except (TypeError, ValueError):
            count = 1
        target_groups = task.get("target_groups") or []

        # 目标群聊 origin
        targets: List[Tuple[str, str]] = []
        platform_ids: List[str] = []
        try:
            for p in self._context.platform_manager.get_insts():
                pid = getattr(p, "platform_id", "")
                if pid:
                    platform_ids.append(pid)
        except Exception:
            pass

        for gid in target_groups:
            gid_str = str(gid).strip()
            umo = self._origin_store.get(gid_str)
            if umo:
                targets.append((umo, gid_str))
                logger.info(f"[girlvideo] 定时任务: 群 {gid_str} 使用缓存 origin")
                continue
            constructed = []
            for pid in platform_ids:
                constructed.append(f"{pid}:GroupMessage:{gid_str}")
            if not constructed:
                constructed.append(f"default:GroupMessage:{gid_str}")
            for umo in constructed:
                targets.append((umo, f"{gid_str}(构造:{umo[:30]})"))
                logger.info(f"[girlvideo] 定时任务: 构造 origin {umo[:50]}")

        if not targets:
            logger.warning("[girlvideo] 定时任务: 无有效目标群聊，跳过搜索")
            return

        mp = _get_media_parser(self._context)
        if mp is None:
            logger.error("[girlvideo] 定时任务: media_parser 未加载")
            return

        # 代理
        proxy_addr = ""
        try:
            proxy_addr = str(getattr(mp.cfg.proxy, "address", "") or "").strip()
        except Exception:
            pass

        # 搜索
        logger.info("[girlvideo] 定时任务: 开始搜索")
        sent_bvids = self._sent_store.load()
        logger.info(f"[girlvideo] 定时任务: 已发送 {len(sent_bvids)} 条")
        order_label = str(task.get("order", "综合排序")).strip()
        task_order = ORDER_MAP.get(order_label, "totalrank")
        results: List[Dict[str, Any]] = []

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60)
        ) as session:
            current_words = keyword.split()
            while current_words:
                search_kw = " ".join(current_words)
                pg = 1
                while pg <= 50:
                    try:
                        page_results, _ = await search_bilibili(
                            session, keyword=search_kw,
                            cookie=self._bilibili_cookie,
                            count=20, order=task_order, page=pg,
                        )
                    except RuntimeError as e:
                        logger.error(f"[girlvideo] 定时任务搜索失败(第{pg}页): {e}")
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
                drop = random.choice(current_words)
                current_words = [w for w in current_words if w != drop]
                logger.info(
                    f"[girlvideo] 定时任务减词回退: "
                    f"移除 '{drop}', 剩余: {current_words}"
                )

        candidates = results[:max(count * 3, 3)]
        if not candidates:
            logger.info("[girlvideo] 定时任务: 无未发送视频，结束")
            return

        logger.info(f"[girlvideo] 定时任务: 搜索到 {len(candidates)} 个候选视频，逐个尝试")

        # 大小限制
        try:
            max_mb = max(0, int(task.get("max_size_mb", 50)))
        except (TypeError, ValueError):
            max_mb = 50

        # 中转注册函数
        _relay_fn = None
        try:
            from astrbot_plugin_media_parser.core.storage import (
                register_files_with_token_service as _relay_fn,
            )
        except ImportError:
            pass

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=300)
        ) as session:
            sent_count = 0
            tried_bvids = []

            for idx, item in enumerate(candidates, 1):
                bvid = str(item.get("bvid", "")).strip()
                if not bvid:
                    continue

                # 时长过滤
                try:
                    max_dur = max(0, int(task.get("max_duration_seconds", 0)))
                except (TypeError, ValueError):
                    max_dur = 0
                if max_dur > 0:
                    dur_str = str(item.get("duration", "")).strip()
                    dur_sec = _parse_duration(dur_str)
                    if dur_sec > max_dur:
                        logger.info(
                            f"[girlvideo] 视频时长 {dur_sec}s"
                            f" > {max_dur}s，换下一个: {bvid}"
                        )
                        tried_bvids.append(bvid)
                        continue

                video_url = f"https://www.bilibili.com/video/{bvid}"
                logger.info(f"[girlvideo] 尝试下载 {idx}/{len(candidates)}: {bvid}")

                # 解析
                try:
                    metadata = await asyncio.wait_for(
                        mp.bilibili_parser.parse_bilibili_minimal(
                            video_url, session=session,
                            cookie_header_override=self._bilibili_cookie or None,
                        ),
                        timeout=90,
                    )
                except (asyncio.TimeoutError, Exception):
                    logger.info(f"[girlvideo] 解析超时/失败，换下一个: {bvid}")
                    tried_bvids.append(bvid)
                    continue

                if not metadata:
                    tried_bvids.append(bvid)
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

                # 大小预检
                if max_mb > 0:
                    first_url = ""
                    for g in stripped:
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
                                    if size_mb > max_mb:
                                        logger.info(
                                            f"[girlvideo] 视频过大 {size_mb:.0f}MB"
                                            f" > {max_mb}MB，换下一个: {bvid}"
                                        )
                                        tried_bvids.append(bvid)
                                        continue
                        except Exception:
                            pass

                # 下载
                try:
                    pm = await asyncio.wait_for(
                        mp.download_manager.process_metadata(
                            session, metadata, proxy_addr=proxy_addr or None,
                        ),
                        timeout=90,
                    )
                except (asyncio.TimeoutError, Exception):
                    logger.info(f"[girlvideo] 下载超时/失败，换下一个: {bvid}")
                    tried_bvids.append(bvid)
                    continue

                fps = pm.get("file_paths") or []
                vms = pm.get("video_modes") or []
                if not any(fp and vm == "local" for fp, vm in zip(fps, vms)):
                    logger.info(f"[girlvideo] 下载失败，换下一个: {bvid}")
                    tried_bvids.append(bvid)
                    continue

                # 发送到每个目标（每个目标独立注册中转，避免 token 单次消费）
                has_local = bool(
                    pm.get("file_paths")
                    and any(fp and vm == "local" for fp, vm in zip(
                        pm.get("file_paths") or [],
                        pm.get("video_modes") or [],
                    ))
                )
                for umo, label in targets:
                    send_pm = pm
                    if has_local and self._relay_enable and self._relay_callback_url and _relay_fn:
                        # 每个目标单独注册，获得独立 token
                        import copy
                        send_pm = copy.deepcopy(pm)
                        try:
                            await _relay_fn(
                                send_pm, self._relay_callback_url, self._relay_ttl,
                            )
                        except Exception:
                            pass

                    use_relay = bool(send_pm.get("use_file_token_service"))
                    urls = (
                        send_pm.get("file_token_urls")
                        if use_relay
                        else send_pm.get("file_paths")
                    )
                    for url in (urls or []):
                        if not url:
                            continue
                        chain = MessageChain()
                        chain.chain = [
                            CompVideo.fromURL(url) if use_relay
                            else CompVideo.fromFileSystem(url)
                        ]
                        try:
                            await self._context.send_message(umo, chain)
                            sent_count += 1
                            logger.info(
                                f"[girlvideo] 定时发送成功: "
                                f"bvid={bvid} target={label}"
                            )
                        except Exception as e:
                            logger.error(
                                f"[girlvideo] 定时发送失败: "
                                f"bvid={bvid} target={label} | {e}"
                            )

                self._sent_store.mark(bvid)
                break

            for bvid in tried_bvids:
                self._sent_store.mark(bvid)

            logger.info(
                f"[girlvideo] 定时任务完成 | 发送: {sent_count} 次"
            )
