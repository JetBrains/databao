import os
import re
from pathlib import Path
from typing import Any

import yaml


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
