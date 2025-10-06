import datetime
import os
import re
from pathlib import Path
from typing import Any

import jinja2
import yaml

from portus.prompts import get_jinja_prompts_env


def get_today_date_str() -> str:
    return datetime.datetime.now().strftime("%A, %Y-%m-%d")


def read_prompt_template(relative_path: Path) -> jinja2.Template:
    env = get_jinja_prompts_env()
    template = env.get_template(str(relative_path))
    return template


def exception_to_string(e: Exception | str) -> str:
    if isinstance(e, str):
        return e
    return f"Exception Name: {type(e).__name__}. Exception Desc: {e}"


def trim_string(s: str, max_length: int | None) -> str:
    if max_length is None:
        return s
    if len(s) > max_length:
        return s[: max_length - 3] + "..."
    return s


def expand_env_vars_str(path: str) -> str:
    """Expand shell variables of the form ${var}. If a variable is not set, an error is raised."""
    # Adapted from https://github.com/python/cpython/blob/3.13/Lib/posixpath.py
    if "${" not in path:
        return path
    pattern = re.compile(r"\$\{([^}]*)\}", re.ASCII)
    i = 0
    while True:
        m = pattern.search(path, i)
        if not m:
            break
        i, j = m.span(0)
        name = m.group(1)
        try:
            value = os.environ[name]
        except KeyError as e:
            raise KeyError(f'Environment variable "{name}" not found') from e
        else:
            tail = path[j:]
            path = path[:i] + value
            i = len(path)
            path += tail
    return path


def expand_env_vars(data: Any) -> Any:
    """Recursively traverses a data structure to expand environment variables as substrings of the form ${VAR}."""
    if isinstance(data, dict):
        return {k: expand_env_vars(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [expand_env_vars(i) for i in data]
    elif isinstance(data, str):
        return expand_env_vars_str(data)
    else:
        return data


def read_config_file(path: Path, *, parse_env_vars: bool = False) -> Any:
    with open(path) as f:
        config = yaml.safe_load(f)
        if parse_env_vars:
            config = expand_env_vars(config)
        return config
