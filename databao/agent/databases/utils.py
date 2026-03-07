from typing import Any


def str_dict(data: dict[str, Any]) -> dict[str, str]:
    return {k: str(v) for k, v in data.items()}


def sql_string_literal(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"
