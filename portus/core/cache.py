from io import BytesIO
from abc import ABC, abstractmethod


class Cache(ABC):
    @abstractmethod
    def put(self, k: str, v: BytesIO) -> None:
        raise NotImplementedError

    @abstractmethod
    def get(self, k: str) -> BytesIO:
        raise NotImplementedError
