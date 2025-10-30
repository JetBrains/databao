from io import BytesIO

from portus.core.cache import Cache


class InMemCache(Cache):
    """Process-local, byte-based cache backed by a dict.

    Use `scoped()` to create namespaced views over the same underlying storage.
    """

    def __init__(self, prefix: str = "", shared_cache: dict[str, bytes] | None = None):
        self._cache: dict[str, bytes] = shared_cache if shared_cache is not None else {}
        self._prefix = prefix

    def put(self, key: str, source: BytesIO) -> None:
        """Store bytes under the current scope/prefix."""
        self._cache[self._prefix + key] = source.getvalue()

    def get(self, key: str, dest: BytesIO) -> None:
        """Write cached bytes for key into the provided buffer."""
        dest.write(self._cache[self._prefix + key])

    def scoped(self, scope: str) -> Cache:
        """Return a view of this cache with an additional scope prefix."""
        return InMemCache(prefix=self._prefix + scope + ":", shared_cache=self._cache)
