import datetime
from pathlib import Path

import jinja2

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
