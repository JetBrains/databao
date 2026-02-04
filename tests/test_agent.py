import collections.abc
import shutil
import uuid
from pathlib import Path

import duckdb
import pandas as pd
import pytest

import databao
from databao.configs import LLMConfigDirectory
from databao.core import AgentV1


@pytest.fixture
def temp_context_file(tmp_path: Path) -> Path:
    """Create a temporary context file with sample content."""
    context_file = tmp_path / "context.md"
    context_file.write_text("This is a test context file for database operations.")
    return context_file


@pytest.fixture
def duckdb_conn(request: pytest.FixtureRequest) -> collections.abc.Generator[duckdb.DuckDBPyConnection, None, None]:
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


def _new_agent() -> AgentV1:
    llm_config = LLMConfigDirectory.DEFAULT.model_copy(update={"model_kwargs": {"api_key": "test"}})
    return databao.new_agent(llm_config=llm_config)


def test_add_db_with_nonexistent_context_path_raises(duckdb_conn: duckdb.DuckDBPyConnection) -> None:
    agent = _new_agent()
    with pytest.raises(FileNotFoundError):
        agent.add_db(duckdb_conn, context=Path("this_file_does_not_exist_123456.md"))


def test_add_df_with_nonexistent_context_path_raises() -> None:
    df = pd.DataFrame({"a": [1, 2, 3]})
    agent = _new_agent()
    with pytest.raises(FileNotFoundError):
        agent.add_df(df, context=Path("another_missing_context_987654.md"))


def test_add_db_with_temp_file_context(temp_context_file: Path, duckdb_conn: duckdb.DuckDBPyConnection) -> None:
    """Test adding a database with context from a temporary file."""
    agent = _new_agent()
    agent.add_db(duckdb_conn, context=temp_context_file)

    assert "db1" in agent.dbs
    assert agent.dbs["db1"].context == temp_context_file.read_text()


def test_add_df_with_temp_file_context(temp_context_file: Path) -> None:
    """Test adding a DataFrame with context from a temporary file."""
    df = pd.DataFrame({"a": [1, 2, 3]})
    agent = _new_agent()
    agent.add_df(df, context=temp_context_file)

    assert "df1" in agent.dfs
    assert agent.dfs["df1"].context == temp_context_file.read_text()


def test_add_db_with_string_context(duckdb_conn: duckdb.DuckDBPyConnection) -> None:
    """Test adding a database with context as a string."""
    agent = _new_agent()
    context_string = "This is a string context for the database."
    agent.add_db(duckdb_conn, context=context_string)

    assert "db1" in agent.dbs
    assert agent.dbs["db1"].context == context_string


def test_add_df_with_string_context() -> None:
    """Test adding a DataFrame with context as a string."""
    df = pd.DataFrame({"a": [1, 2, 3]})
    agent = _new_agent()
    context_string = "This is a string context for the DataFrame."
    agent.add_df(df, context=context_string)

    assert "df1" in agent.dfs
    assert agent.dfs["df1"].context == context_string


def test_add_additional_context_with_nonexistent_path_raises() -> None:
    """add_additional_context should raise if given a non-existent Path."""
    agent = _new_agent()
    with pytest.raises(FileNotFoundError):
        agent.add_context(Path("no_such_context_file_123.md"))


def test_add_additional_context_with_temp_file(temp_context_file: Path) -> None:
    """Ensure additional context can be loaded from a temporary file path."""
    agent = _new_agent()
    agent.add_context(temp_context_file)
    assert agent.additional_context == [temp_context_file.read_text()]


def test_add_additional_context_with_string() -> None:
    """Ensure additional context can be provided directly as a string."""
    agent = _new_agent()
    text = "Global instructions for the agent go here."
    agent.add_context(text)
    assert agent.additional_context == [text]


def test_add_additional_context_multiple_calls_mixed_sources(temp_context_file: Path) -> None:
    """Calling add_additional_context multiple times should append in order."""
    agent = _new_agent()
    first = "First global instruction."
    second = temp_context_file.read_text()
    third = "Third bit of context."

    agent.add_context(first)
    agent.add_context(temp_context_file)
    agent.add_context(third)

    assert first in agent.additional_context
    assert second in agent.additional_context
    assert third in agent.additional_context
