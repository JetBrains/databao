from pathlib import Path

import jinja2

_jinja_prompts_env: jinja2.Environment | None = None


def get_jinja_prompts_env(prompts_dir: Path | None = None) -> jinja2.Environment:
    if prompts_dir:
        return jinja2.Environment(loader=jinja2.FileSystemLoader(prompts_dir))

    global _jinja_prompts_env
    if _jinja_prompts_env is None:
        # A package loader must be used for using as a library!
        _jinja_prompts_env = jinja2.Environment(
            loader=jinja2.PackageLoader("portus", "prompts"),
            trim_blocks=True,  # better whitespace handling
            lstrip_blocks=True,
        )
    return _jinja_prompts_env
