import json
import pickle
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from databao.core import Cache

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS cache (
    key   TEXT PRIMARY KEY,
    value BLOB NOT NULL,
    tag   TEXT
)
"""
_CREATE_INDEX = "CREATE INDEX IF NOT EXISTS idx_cache_tag ON cache(tag)"


@dataclass(kw_only=True)
class DiskCacheConfig:
    db_dir: str | Path = Path("cache/diskcache/")


class DiskCache(Cache):
    """A simple SQLite-backed cache."""

    def __init__(
        self,
        config: DiskCacheConfig | None = None,
        conn: sqlite3.Connection | None = None,
        prefix: str = "",
    ):
        self.config = config or DiskCacheConfig()
        if conn is not None:
            self._conn = conn
        else:
            db_dir = Path(self.config.db_dir)
            db_dir.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(str(db_dir / "cache.db"), check_same_thread=False)
        self._conn.execute(_CREATE_TABLE)
        self._conn.execute(_CREATE_INDEX)
        self._conn.commit()
        self._prefix = prefix

    def put(self, key: str, state: dict[str, Any]) -> None:
        k = f"{self._prefix}{key}"
        self._conn.execute(
            "INSERT OR REPLACE INTO cache (key, value, tag) VALUES (?, ?, ?)",
            (k, pickle.dumps(state), self._prefix),
        )
        self._conn.commit()

    def get(self, key: str, default: dict[str, Any] | None = None) -> dict[str, Any]:
        k = f"{self._prefix}{key}"
        row = self._conn.execute("SELECT value FROM cache WHERE key = ?", (k,)).fetchone()
        if row is None:
            _default: dict[str, Any] = {} if default is None else default
            return _default
        result: dict[str, Any] = pickle.loads(row[0])
        return result

    def scoped(self, scope: str) -> "DiskCache":
        return DiskCache(self.config, self._conn, prefix=f"{self._prefix}/{scope}/")

    def __contains__(self, key: str) -> bool:
        row = self._conn.execute("SELECT 1 FROM cache WHERE key = ?", (key,)).fetchone()
        return row is not None

    @staticmethod
    def make_json_key(d: dict[str, Any]) -> str:
        # Keep the key human-readable at the cost of some cache size and performance.
        return json.dumps(d, sort_keys=True)

    def close(self) -> None:
        self._conn.close()

    def invalidate_tag(self, tag: str) -> int:
        cursor = self._conn.execute("DELETE FROM cache WHERE tag = ?", (tag,))
        self._conn.commit()
        return cursor.rowcount
