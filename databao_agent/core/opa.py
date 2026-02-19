from dataclasses import dataclass


@dataclass(frozen=True)
class Opa:
    """User question to the LLM"""

    query: str
