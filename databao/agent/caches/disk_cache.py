import json
import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import diskcache  # type: ignore[import-untyped]

from databao.agent.core import Cache


@dataclass(kw_only=True)
class DiskCacheConfig:
    db_dir: str | Path = Path("cache/diskcache/")


class DiskCache(Cache):
    """A simple SQLite-backed cache."""

    def __init__(self, config: DiskCacheConfig | None = None, cache: diskcache.Cache | None = None, prefix: str = ""):
        self.config = config or DiskCacheConfig()
        self._cache: diskcache.Cache = cache or diskcache.Cache(str(self.config.db_dir))
        self._prefix = prefix

    def put(self, key: str, state: dict[str, Any]) -> None:
        k = f"{self._prefix}{key}"
        self._cache.set(k, value=pickle.dumps(state), tag=self._prefix)

    def get(self, key: str, default: dict[str, Any] | None = None) -> dict[str, Any]:
        k = f"{self._prefix}{key}"
        res_bytes = self._cache.get(k, default=None)
        if res_bytes is None:
            _default: dict[str, Any] = {} if default is None else default
            return _default
        result: dict[str, Any] = pickle.loads(res_bytes)
        return result

    def scoped(self, scope: str) -> "DiskCache":
        return DiskCache(self.config, self._cache, prefix=f"{self._prefix}/{scope}/")

    def __contains__(self, key: str) -> bool:
        return key in self._cache

    @staticmethod
    def make_json_key(d: dict[str, Any]) -> str:
        # Keep the key human-readable at the cost of some cache size and performance.
        return json.dumps(d, sort_keys=True)

    def close(self) -> None:
        self._cache.close()

    def invalidate_tag(self, tag: str) -> int:
        n_evicted: int = self._cache.evict(tag=tag)
        return n_evicted
