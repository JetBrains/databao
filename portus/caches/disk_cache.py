import json
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any

import diskcache  # type: ignore[import-untyped]
import pandas as pd

from portus.core import Cache

DEFAULT_CACHE_DIR = Path("cache/diskcache/")


@dataclass(kw_only=True)
class DiskCacheConfig:
    db_dir: str | Path = DEFAULT_CACHE_DIR


class _DiskCache:
    """A simple SQLite-backed cache."""

    def __init__(self, config: DiskCacheConfig):
        self.config = config
        self._cache = diskcache.Cache(str(self.config.db_dir))

    def __contains__(self, key: str) -> bool:
        return key in self._cache

    @staticmethod
    def make_json_key(d: dict[str, Any]) -> str:
        # Keep the key human-readable at the cost of some cache size and performance.
        return json.dumps(d, sort_keys=True)

    @classmethod
    def _make_sql_key(cls, sql: str, source_name: str) -> str:
        return cls.make_json_key({"sql": sql, "source_name": source_name})

    @staticmethod
    def _get_sql_tag(source_name: str) -> str:
        return f"{source_name}/sql"

    def set_sql(self, sql: str, value: pd.DataFrame, *, source_name: str) -> None:
        """Store a SQL that was executed in a named data source and its resulting DataFrame in the cache.

        Use this instead of `set` for convenience and consistency when caching SQL results.
        """
        # DataFrames are serialized losslessly as pickle blobs.
        # We are using pickle for now because serializing DataFrames is tricky. We can have:
        #  - duplicate column names, empty string column names, various data types, etc.
        key = self._make_sql_key(sql, source_name)
        self.set(key, value, tag=self._get_sql_tag(source_name))

    def get_sql(self, sql: str, *, source_name: str) -> pd.DataFrame | None:
        key = self._make_sql_key(sql, source_name)
        val: pd.DataFrame | None = self.get(key)
        return val

    def invalidate_source_if_stale_query(
        self, *, source_name: str, is_stale_query: str, is_stale_query_df: pd.DataFrame
    ) -> None:
        """Use is_stale_query to check if the cached results need to be updated.
        If the data is stale, all queries for the given source will be invalidated.

        The is_stale_query should return whether the data is stale or not (e.g., the latest timestamp).
        `is_stale_query` should always be computed against the live database, without any caching.
        """
        cached_df = self.get_sql(is_stale_query, source_name=source_name)
        if cached_df is None or not is_stale_query_df.equals(cached_df):
            self.invalidate_tag(
                self._get_sql_tag(source_name)
            )  # Invalidate in case the is_stale_query has changed since last time
            self.set_sql(is_stale_query, is_stale_query_df, source_name=source_name)

    def set(self, key: str, value: Any, ttl_seconds: float | None = None, tag: str | None = None) -> None:
        """Store a value of any type in the cache.

        Simple types for value will be stored as-is, while complex types will be pickled.
        Assign a tag to be able to delete by tag later.
        """
        # N.B. The key could also be pickled (it doesn't have to be a string), but it's better
        # to force having string/int keys.
        self._cache.set(key, value, expire=ttl_seconds, tag=tag)

    def get(self, key: str, default: Any = None) -> Any:
        return self._cache.get(key, default=default)

    def close(self) -> None:
        self._cache.close()

    def invalidate_tag(self, tag: str) -> int:
        n_evicted: int = self._cache.evict(tag=tag)
        return n_evicted


class DiskCache(Cache):
    def __init__(self, cache: _DiskCache, prefix: str = ""):
        self._cache = cache
        self._prefix = prefix

    def put(self, key: str, source: BytesIO) -> None:
        self._cache.set(key, value=source.getvalue(), tag=self._prefix)

    def get(self, key: str, dest: BytesIO) -> None:
        val = self._cache.get(key, default=None)
        if val is None:
            raise KeyError(f"Key {key} not found in cache.")
        dest.write(val)

    def scoped(self, scope: str) -> "DiskCache":
        return DiskCache(self._cache, prefix=self._prefix + scope + ":")
