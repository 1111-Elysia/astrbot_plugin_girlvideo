"""持久化存储：已发送 BVid 记录 + 群聊 origin 缓存。"""
import json
import os
from pathlib import Path
from typing import Dict, Set


class SentRecordStore:
    """已发送 BVid 记录，JSON 文件持久化，Set 去重。"""

    def __init__(self, data_dir: Path):
        self._data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)

    @property
    def _file(self) -> Path:
        return self._data_dir / "sent_bvids.json"

    def load(self) -> Set[str]:
        if not self._file.exists():
            return set()
        try:
            data = json.loads(self._file.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return {str(v) for v in data if v}
            return set()
        except Exception:
            return set()

    def mark(self, bvid: str) -> None:
        existing = self.load()
        existing.add(str(bvid).strip())
        self._file.write_text(
            json.dumps(sorted(existing), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def count(self) -> int:
        return len(self.load())


class OriginStore:
    """群聊 unified_msg_origin 缓存，JSON 持久化。"""

    def __init__(self, data_dir: Path):
        self._data_dir = data_dir
        self._cache: Dict[str, str] = {}
        os.makedirs(data_dir, exist_ok=True)
        self._restore()

    @property
    def _file(self) -> Path:
        return self._data_dir / "group_origins.json"

    def _restore(self) -> None:
        try:
            if self._file.exists():
                self._cache = json.loads(
                    self._file.read_text(encoding="utf-8")
                )
        except Exception:
            pass

    def _save(self) -> None:
        try:
            self._file.write_text(
                json.dumps(self._cache, ensure_ascii=False),
                encoding="utf-8",
            )
        except Exception:
            pass

    def get(self, group_id: str) -> str:
        return self._cache.get(str(group_id), "")

    def set(self, group_id: str, origin: str) -> bool:
        """缓存 origin。返回 True 表示首次缓存或值有变化。"""
        gid = str(group_id)
        old = self._cache.get(gid)
        if old == origin:
            return False
        self._cache[gid] = origin
        self._save()
        return True

    @property
    def count(self) -> int:
        return len(self._cache)


def get_data_dir() -> Path:
    """获取插件数据目录。"""
    try:
        from astrbot.core.utils.astrbot_path import (
            get_astrbot_plugin_data_path,
        )
        return Path(get_astrbot_plugin_data_path()) / "astrbot_plugin_girlvideo"
    except Exception:
        return Path(os.getcwd()) / "data"
