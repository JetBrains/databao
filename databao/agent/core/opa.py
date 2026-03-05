from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Opa:
    """User question to the LLM"""

    query: str
    metadata: dict[str, Any] | None = None
    tags: list[str] | None = None
