"""Context extraction from DCE (nemory) output files.

Nemory output structure:
    output/run-<timestamp>/
        all_results.yaml          # All results combined
        databases/                # Database introspection results
            <name>.yaml
        files/                    # File processing results
            <name>.yaml
        dbt-introspections/       # dbt introspection results (if configured)
            <name>.yaml

Each result file contains:
    name: datasource name
    type: full type (e.g., "databases/duckdb")
    result: introspection data with catalogs > schemas > tables

NOTE: We pass raw DCE context to Databao without post-processing.
DCE already produces properly formatted context that Databao can use directly.
"""
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass
class DatabaseContext:
    """Context information for a database."""

    database_id: str
    context_text: str


@dataclass
class FileContext:
    """Context information from file sources."""

    file_id: str
    name: str
    context_text: str


def load_yaml_file(path: Path) -> dict[str, Any] | None:
    """Load a YAML file safely."""
    try:
        with open(path) as f:
            result = yaml.safe_load(f)
            if isinstance(result, dict):
                return result
            return None
    except (OSError, yaml.YAMLError):
        return None


def serialize_context_to_yaml(data: dict[str, Any]) -> str:
    """Serialize DCE context data to YAML string for Databao.

    We pass the raw DCE context without post-processing.
    DCE already produces well-structured context that Databao can use directly.
    """
    return yaml.dump(data, default_flow_style=False, allow_unicode=True, sort_keys=False)


def extract_database_contexts(run_dir: Path) -> list[DatabaseContext]:
    """Extract context from database introspection files.

    Looks for result files in run_dir/databases/*.yaml
    Passes raw DCE context to Databao without post-processing.
    """
    contexts: list[DatabaseContext] = []

    databases_dir = run_dir / "databases"
    if not databases_dir.is_dir():
        return contexts

    for db_file in databases_dir.glob("*.yaml"):
        data = load_yaml_file(db_file)
        if not data:
            continue

        db_id = data.get("name", db_file.stem)
        # Pass raw DCE context - no post-processing needed
        context_text = serialize_context_to_yaml(data)
        contexts.append(DatabaseContext(database_id=db_id, context_text=context_text))

    return contexts


def extract_file_contexts(run_dir: Path) -> list[FileContext]:
    """Extract context from file processing results.

    Looks for result files in run_dir/files/*.yaml
    Passes raw DCE context to Databao without post-processing.
    """
    contexts: list[FileContext] = []

    files_dir = run_dir / "files"
    if not files_dir.is_dir():
        return contexts

    for file_path in files_dir.glob("*.yaml"):
        data = load_yaml_file(file_path)
        if not data:
            continue

        file_id = data.get("name", file_path.stem)
        # Pass raw DCE context - no post-processing needed
        context_text = serialize_context_to_yaml(data)
        contexts.append(
            FileContext(
                file_id=file_id,
                name=data.get("name", file_path.stem),
                context_text=context_text,
            )
        )

    return contexts


def get_all_context(run_dir: Path) -> tuple[list[DatabaseContext], list[FileContext]]:
    """Get all context from a nemory run directory.

    Returns:
        Tuple of (database_contexts, file_contexts)
    """
    db_contexts = extract_database_contexts(run_dir)
    file_contexts = extract_file_contexts(run_dir)
    return db_contexts, file_contexts
