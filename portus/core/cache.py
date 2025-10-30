from abc import ABC, abstractmethod
from io import BytesIO


class Cache(ABC):
     """Simple byte-oriented cache interface with optional scoping."""

    @abstractmethod
    def put(self, key: str, source: BytesIO) -> None:
        """Store bytes for a key from the given buffer."""
        raise NotImplementedError

    @abstractmethod
    def get(self, key: str, dest: BytesIO) -> None:
        """Load bytes for a key into the provided buffer.

        Implementations may raise KeyError if the key is missing.
        """
        # TODO Raise KeyError if key not found. Need a "contains" method as well.
        raise NotImplementedError

    @abstractmethod
    def scoped(self, scope: str) -> "Cache":
        """Return a new cache view with the given key prefix/scope."""
        raise NotImplementedError
