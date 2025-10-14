from io import BytesIO

from portus.core.cache import Cache


class InMemCache(Cache):
    def __init__(self, prefix: str = ""):
        self._cache: dict[str, bytes] = {}
        self._prefix = prefix

    def put(self, key: str, source: BytesIO) -> None:
        self._cache[self._prefix + key] = source.getvalue()

    def get(self, key: str, dest: BytesIO) -> None:
        dest.write(self._cache[self._prefix + key])

    def scoped(self, scope: str) -> Cache:
        return InMemCache(prefix=scope)
