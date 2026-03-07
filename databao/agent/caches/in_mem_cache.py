from typing import Any

from databao.agent.core.cache import Cache


class InMemCache(Cache):
    """Process-local, dict-based cache.

    Use `scoped()` to create namespaced views over the same underlying storage.
    """

    def __init__(self, prefix: str = "", shared_cache: dict[str, dict[str, Any]] | None = None):
        self._cache: dict[str, dict[str, Any]] = shared_cache if shared_cache is not None else {}
        self._prefix = prefix

    def put(self, key: str, state: dict[str, Any]) -> None:
        """Store bytes under the current scope/prefix."""
        self._cache[self._prefix + key] = state

    def get(self, key: str, default: dict[str, Any] | None = None) -> dict[str, Any]:
        """Write cached state for key."""
        default = {} if default is None else default
        return self._cache.get(self._prefix + key, default)

    def scoped(self, scope: str) -> Cache:
        """Return a view of this cache with an additional scope prefix."""
        return InMemCache(prefix=self._prefix + scope + ":", shared_cache=self._cache)
