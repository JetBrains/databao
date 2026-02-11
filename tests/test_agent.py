import collections.abc
import shutil
import uuid
from pathlib import Path

import duckdb
import pandas as pd
import pytest
from _duckdb import DuckDBPyConnection

import databao
from databao.configs import LLMConfigDirectory
from databao.core.agent import Agent
from databao.core.context import Context, ContextBuilder


@pytest.fixture
def temp_context_file(tmp_path: Path) -> Path:
    """Create a temporary context file with sample content."""
    context_file = tmp_path / "context.md"
    context_file.write_text("This is a test context file for database operations.")
    return context_file


@pytest.fixture
def duckdb_conn(request: pytest.FixtureRequest) -> collections.abc.Generator[DuckDBPyConnection, None, None]:
    root = Path(request.config.rootpath)
    base = root / "tests/.pytest-artifacts"
    base.mkdir(parents=True, exist_ok=True)

    path = base / f"duckdb-{request.node.name}-{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=False)

    src = root / "examples/web_shop_orders/data/web_shop.duckdb"
    dst = path / "web_shop.duckdb"
    shutil.copy2(src, dst)
    conn = duckdb.connect(str(dst))
    try:
        yield conn
    finally:
        conn.close()
        shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def builder() -> ContextBuilder:
    return Context.builder()


def _new_agent(context: Context) -> Agent:
    llm_config = LLMConfigDirectory.DEFAULT.model_copy(update={"model_kwargs": {"api_key": "test"}})
    return databao.agent(context, llm_config=llm_config)


def test_add_db_with_nonexistent_context_path_raises(builder: ContextBuilder, duckdb_conn: DuckDBPyConnection) -> None:
    with pytest.raises(FileNotFoundError):
        builder.add_db(duckdb_conn, context=Path("this_file_does_not_exist_123456.md"))


def test_add_df_with_nonexistent_context_path_raises(
    builder: ContextBuilder,
) -> None:
    df = pd.DataFrame({"a": [1, 2, 3]})

    with pytest.raises(FileNotFoundError):
        builder.add_df(df, context=Path("another_missing_context_987654.md"))


def test_add_db_with_temp_file_context(
    builder: ContextBuilder, temp_context_file: Path, duckdb_conn: DuckDBPyConnection
) -> None:
    """Test adding a database with context from a temporary file."""
    builder.add_db(duckdb_conn, context=temp_context_file)
    context = builder.build()
    agent = _new_agent(context)

    assert "db1" in agent.dbs
    assert agent.dbs["db1"].context == temp_context_file.read_text()


def test_add_df_with_temp_file_context(builder: ContextBuilder, temp_context_file: Path) -> None:
    """Test adding a DataFrame with context from a temporary file."""
    df = pd.DataFrame({"a": [1, 2, 3]})

    builder.add_df(df, context=temp_context_file)
    context = builder.build()
    agent = _new_agent(context)

    assert "df1" in agent.dfs
    assert agent.dfs["df1"].context == temp_context_file.read_text()


def test_add_db_with_string_context(builder: ContextBuilder, duckdb_conn: DuckDBPyConnection) -> None:
    """Test adding a database with context as a string."""
    context_string = "This is a string context for the database."

    builder.add_db(duckdb_conn, context=context_string)
    context = builder.build()
    agent = _new_agent(context)

    assert "db1" in agent.dbs
    assert agent.dbs["db1"].context == context_string


def test_add_df_with_string_context(builder: ContextBuilder) -> None:
    """Test adding a DataFrame with context as a string."""
    df = pd.DataFrame({"a": [1, 2, 3]})
    context_string = "This is a string context for the DataFrame."

    builder.add_df(df, context=context_string)
    context = builder.build()
    agent = _new_agent(context)

    assert "df1" in agent.dfs
    assert agent.dfs["df1"].context == context_string


def test_add_additional_context_with_nonexistent_path_raises(builder: ContextBuilder) -> None:
    """add_additional_context should raise if given a non-existent Path."""
    with pytest.raises(FileNotFoundError):
        builder.add_context(Path("no_such_context_file_123.md"))


def test_add_additional_context_with_temp_file(builder: ContextBuilder, temp_context_file: Path) -> None:
    """Ensure additional context can be loaded from a temporary file path."""
    builder.add_context(temp_context_file)
    context = builder.build()
    agent = _new_agent(context)

    assert agent.additional_context == [temp_context_file.read_text()]


def test_add_additional_context_with_string(builder: ContextBuilder) -> None:
    """Ensure additional context can be provided directly as a string."""
    text = "Global instructions for the agent go here."

    builder.add_context(text)
    context = builder.build()
    agent = _new_agent(context)

    assert agent.additional_context == [text]


def test_add_additional_context_multiple_calls_mixed_sources(builder: ContextBuilder, temp_context_file: Path) -> None:
    """Calling add_additional_context multiple times should append in order."""
    first = "First global instruction."
    second = temp_context_file.read_text()
    third = "Third bit of context."

    builder.add_context(first)
    builder.add_context(temp_context_file)
    builder.add_context(third)
    context = builder.build()
    agent = _new_agent(context)

    assert first in agent.additional_context
    assert second in agent.additional_context
    assert third in agent.additional_context
