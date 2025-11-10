import json
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any

import diskcache  # type: ignore[import-untyped]

from databao.core import Cache


@dataclass(kw_only=True)
class DiskCacheConfig:
    db_dir: str | Path = Path("cache/diskcache/")


class DiskCache(Cache):
    """A simple SQLite-backed cache."""

    def __init__(self, config: DiskCacheConfig, cache: diskcache.Cache | None = None, prefix: str = ""):
        self.config = config
        self._cache = cache or diskcache.Cache(str(self.config.db_dir))
        self._prefix = prefix

    def put(self, key: str, source: BytesIO) -> None:
        k = f"{self._prefix}{key}"
        self._cache.set_object(k, value=source.getvalue(), tag=self._prefix)

    def get(self, key: str, dest: BytesIO) -> None:
        k = f"{self._prefix}{key}"
        val = self._cache.get_object(k, default=None)
        if val is None:
            raise KeyError(f"Key {key} not found in cache.")
        dest.write(val)

    def scoped(self, scope: str) -> "DiskCache":
        return DiskCache(self.config, self._cache, prefix=f"{self._prefix}/{scope}/")

    def __contains__(self, key: str) -> bool:
        return key in self._cache

    @staticmethod
    def make_json_key(d: dict[str, Any]) -> str:
        # Keep the key human-readable at the cost of some cache size and performance.
        return json.dumps(d, sort_keys=True)

    def set_object(self, key: str, value: Any, ttl_seconds: float | None = None, tag: str | None = None) -> None:
        """Store a value of any type in the cache.

        Simple types for value will be stored as-is, while complex types will be pickled.
        Assign a tag to be able to delete by tag later.
        """
        # N.B. The key could also be pickled (it doesn't have to be a string), but it's better
        # to force having string/int keys.
        self._cache.set(key, value, expire=ttl_seconds, tag=tag)

    def get_object(self, key: str, default: Any = None) -> Any:
        return self._cache.get(key, default=default)

    def close(self) -> None:
        self._cache.close()

    def invalidate_tag(self, tag: str) -> int:
        n_evicted: int = self._cache.evict(tag=tag)
        return n_evicted
