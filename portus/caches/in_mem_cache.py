from io import BytesIO

from portus.core.cache import Cache


class InMemCache(Cache):
    def __init__(self):
        self._cache = {}

    def put(self, k: str, v: BytesIO) -> None:
        self._cache[k] = v

    def get(self, k: str) -> BytesIO:
        return self._cache[k]
