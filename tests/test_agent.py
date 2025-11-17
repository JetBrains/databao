from pathlib import Path

import duckdb
import pandas as pd
import pytest

import databao


@pytest.fixture(autouse=True)
def set_dummy_openai_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set a dummy OPENAI_API_KEY environment variable for tests in this file."""
    monkeypatch.setenv("OPENAI_API_KEY", "dummy-key-for-testing")


@pytest.fixture
def temp_context_file(tmp_path: Path) -> Path:
    """Create a temporary context file with sample content."""
    context_file = tmp_path / "context.md"
    context_file.write_text("This is a test context file for database operations.")
    return context_file


@pytest.fixture
def duckdb_conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect("./examples/data/web_shop.duckdb")


def test_add_db_with_nonexistent_context_path_raises(duckdb_conn: duckdb.DuckDBPyConnection) -> None:
    agent = databao.new_agent("ctx_path_agent_db")
    with pytest.raises(FileNotFoundError):
        agent.add_db(duckdb_conn, context=Path("this_file_does_not_exist_123456.md"))


def test_add_df_with_nonexistent_context_path_raises() -> None:
    df = pd.DataFrame({"a": [1, 2, 3]})
    agent = databao.new_agent("ctx_path_agent_df")
    with pytest.raises(FileNotFoundError):
        agent.add_df(df, context=Path("another_missing_context_987654.md"))


def test_add_db_with_temp_file_context(temp_context_file: Path, duckdb_conn: duckdb.DuckDBPyConnection) -> None:
    """Test adding a database with context from a temporary file."""
    agent = databao.new_agent("ctx_path_agent_db_file")
    agent.add_db(duckdb_conn, context=temp_context_file)

    assert "db1" in agent.db_context
    assert agent.db_context["db1"] == temp_context_file.read_text()


def test_add_df_with_temp_file_context(temp_context_file: Path) -> None:
    """Test adding a DataFrame with context from a temporary file."""
    df = pd.DataFrame({"a": [1, 2, 3]})
    agent = databao.new_agent("ctx_path_agent_df_file")
    agent.add_df(df, context=temp_context_file)

    assert "df1" in agent.df_context
    assert agent.df_context["df1"] == temp_context_file.read_text()


def test_add_db_with_string_context(duckdb_conn: duckdb.DuckDBPyConnection) -> None:
    """Test adding a database with context as a string."""
    agent = databao.new_agent("ctx_string_agent_db")
    context_string = "This is a string context for the database."
    agent.add_db(duckdb_conn, context=context_string)

    assert "db1" in agent.db_context
    assert agent.db_context["db1"] == context_string


def test_add_df_with_string_context() -> None:
    """Test adding a DataFrame with context as a string."""
    df = pd.DataFrame({"a": [1, 2, 3]})
    agent = databao.new_agent("ctx_string_agent_df")
    context_string = "This is a string context for the DataFrame."
    agent.add_df(df, context=context_string)

    assert "df1" in agent.df_context
    assert agent.df_context["df1"] == context_string


def test_add_additional_context_with_nonexistent_path_raises() -> None:
    """add_additional_context should raise if given a non-existent Path."""
    agent = databao.new_agent("additional_ctx_missing_path")
    with pytest.raises(FileNotFoundError):
        agent.add_context(Path("no_such_context_file_123.md"))


def test_add_additional_context_with_temp_file(temp_context_file: Path) -> None:
    """Ensure additional context can be loaded from a temporary file path."""
    agent = databao.new_agent("additional_ctx_from_file")
    agent.add_context(temp_context_file)
    assert agent.additional_context == [temp_context_file.read_text()]


def test_add_additional_context_with_string() -> None:
    """Ensure additional context can be provided directly as a string."""
    agent = databao.new_agent("additional_ctx_from_string")
    text = "Global instructions for the agent go here."
    agent.add_context(text)
    assert agent.additional_context == [text]


def test_add_additional_context_multiple_calls_mixed_sources(temp_context_file: Path) -> None:
    """Calling add_additional_context multiple times should append in order."""
    agent = databao.new_agent("additional_ctx_multiple")
    first = "First global instruction."
    second = temp_context_file.read_text()
    third = "Third bit of context."

    agent.add_context(first)
    agent.add_context(temp_context_file)
    agent.add_context(third)

    assert first in agent.additional_context
    assert second in agent.additional_context
    assert third in agent.additional_context
