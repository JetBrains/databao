from __future__ import annotations

import datetime
from collections.abc import Callable
from typing import TYPE_CHECKING

import jinja2

if TYPE_CHECKING:
    from databao.agent.core.data_source import Sources


def get_today_date_str() -> str:
    """Return today's date as a formatted string."""
    return datetime.datetime.now().strftime("%A, %Y-%m-%d")


_jinja_envs: dict[str, jinja2.Environment] = {}


def load_prompt_template(package: str, template_name: str) -> jinja2.Template:
    """Load a Jinja2 template from a Python package directory.

    Args:
        package: Dotted package path, e.g. "databao.executors.lighthouse"
        template_name: File name inside the package, e.g. "system_prompt.jinja"
    """
    if package not in _jinja_envs:
        _jinja_envs[package] = jinja2.Environment(
            loader=jinja2.PackageLoader(package, ""),
            trim_blocks=True,
            lstrip_blocks=True,
        )
    return _jinja_envs[package].get_template(template_name)


def build_context_text(
    sources: Sources,
    *,
    include_dbts: bool = False,
    df_label_fn: _DFLabelFn | None = None,
) -> str:
    """Assemble context text from data sources for injection into system prompts.

    Args:
        sources: The Sources object containing dbs, dfs, dbts, additional_description.
        include_dbts: Whether to include DBT source descriptions (only relevant for dbt executor).
        df_label_fn: Optional callable ``(name) -> str`` that returns the label for a
            DataFrame source. Defaults to ``"DF {name}"``.
    """
    parts: list[str] = []

    for name, source in sources.dbs.items():
        if source.description:
            parts.append(f"## Context for DB {name}\n\n{source.description}\n\n")

    for name, source in sources.dfs.items():
        if source.description:
            label = df_label_fn(name) if df_label_fn else f"DF {name}"
            parts.append(f"## Context for {label}\n\n{source.description}\n\n")

    if include_dbts:
        for name, source in sources.dbts.items():
            if source.description:
                parts.append(f"## Context for DBT {name}\n\n{source.description}\n\n")

    for idx, add_ctx in enumerate(sources.additional_description, start=1):
        parts.append(f"## General information {idx}\n\n{add_ctx.strip()}\n\n")

    return "".join(parts).strip()


_DFLabelFn = Callable[[str], str]
