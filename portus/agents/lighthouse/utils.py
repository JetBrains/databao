import datetime
from pathlib import Path

import jinja2

_jinja_prompts_env: jinja2.Environment | None = None


def get_today_date_str() -> str:
    return datetime.datetime.now().strftime("%A, %Y-%m-%d")


def read_prompt_template(relative_path: Path) -> jinja2.Template:
    env = _get_jinja_prompts_env()
    template = env.get_template(str(relative_path))
    return template


def exception_to_string(e: Exception | str) -> str:
    if isinstance(e, str):
        return e
    return f"Exception Name: {type(e).__name__}. Exception Desc: {e}"


def _get_jinja_prompts_env(prompts_dir: Path | None = None) -> jinja2.Environment:
    if prompts_dir:
        return jinja2.Environment(loader=jinja2.FileSystemLoader(prompts_dir))

    global _jinja_prompts_env
    if _jinja_prompts_env is None:
        # A package loader must be used for using as a library!
        # Use empty string to load from package directory itself, not from 'templates' subdirectory
        _jinja_prompts_env = jinja2.Environment(
            loader=jinja2.PackageLoader("portus.agents.lighthouse", ""),
            trim_blocks=True,  # better whitespace handling
            lstrip_blocks=True,
        )
    return _jinja_prompts_env
