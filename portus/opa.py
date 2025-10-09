from dataclasses import dataclass


@dataclass(frozen=True)
class Opa:
    query: str
