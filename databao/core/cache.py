from abc import ABC, abstractmethod
from io import BytesIO


class Cache(ABC):
    @abstractmethod
    def put(self, key: str, source: BytesIO) -> None:
        raise NotImplementedError

    @abstractmethod
    def get(self, key: str, dest: BytesIO) -> None:
        # TODO Raise KeyError if key not found. Need a "contains" method as well.
        raise NotImplementedError

    @abstractmethod
    def scoped(self, scope: str) -> "Cache":
        raise NotImplementedError
