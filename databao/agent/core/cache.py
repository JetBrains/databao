from abc import ABC, abstractmethod
from typing import Any


class Cache(ABC):
    """Simple State cache interface with optional scoping."""

    @abstractmethod
    def put(self, key: str, state: dict[str, Any]) -> None:
        """Store state for a key."""
        raise NotImplementedError

    @abstractmethod
    def get(self, key: str, default: dict[str, Any] | None = None) -> dict[str, Any]:
        """Load cached state for a key.

        Returns default value if the key is missing.
        """
        raise NotImplementedError

    @abstractmethod
    def scoped(self, scope: str) -> "Cache":
        """Return a new cache view with the given key prefix/scope."""
        raise NotImplementedError
