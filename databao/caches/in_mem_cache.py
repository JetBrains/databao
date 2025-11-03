from io import BytesIO

from databao.core.cache import Cache


class InMemCache(Cache):
    def __init__(self, prefix: str = "", shared_cache: dict[str, bytes] | None = None):
        self._cache: dict[str, bytes] = shared_cache if shared_cache is not None else {}
        self._prefix = prefix

    def put(self, key: str, source: BytesIO) -> None:
        self._cache[self._prefix + key] = source.getvalue()

    def get(self, key: str, dest: BytesIO) -> None:
        dest.write(self._cache[self._prefix + key])

    def scoped(self, scope: str) -> Cache:
        return InMemCache(prefix=self._prefix + scope + ":", shared_cache=self._cache)
