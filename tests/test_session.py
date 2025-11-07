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


def test_add_db_with_nonexistent_context_path_raises() -> None:
    conn = duckdb.connect(":memory:")
    session = databao.open_session("ctx_path_session_db")
    with pytest.raises(FileNotFoundError):
        session.add_db(conn, context=Path("this_file_does_not_exist_123456.md"))


def test_add_df_with_nonexistent_context_path_raises() -> None:
    df = pd.DataFrame({"a": [1, 2, 3]})
    session = databao.open_session("ctx_path_session_df")
    with pytest.raises(FileNotFoundError):
        session.add_df(df, context=Path("another_missing_context_987654.md"))


def test_add_db_with_temp_file_context(temp_context_file: Path) -> None:
    """Test adding a database with context from a temporary file."""
    conn = duckdb.connect(":memory:")
    session = databao.open_session("ctx_path_session_db_file")
    session.add_db(conn, context=temp_context_file)

    db_contexts, _df_contexts = session.context
    assert "db1" in db_contexts
    assert db_contexts["db1"] == temp_context_file.read_text()


def test_add_df_with_temp_file_context(temp_context_file: Path) -> None:
    """Test adding a DataFrame with context from a temporary file."""
    df = pd.DataFrame({"a": [1, 2, 3]})
    session = databao.open_session("ctx_path_session_df_file")
    session.add_df(df, context=temp_context_file)

    _db_contexts, df_contexts = session.context
    assert "df1" in df_contexts
    assert df_contexts["df1"] == temp_context_file.read_text()


def test_add_db_with_string_context() -> None:
    """Test adding a database with context as a string."""
    conn = duckdb.connect(":memory:")
    session = databao.open_session("ctx_string_session_db")
    context_string = "This is a string context for the database."
    session.add_db(conn, context=context_string)

    db_contexts, _df_contexts = session.context
    assert "db1" in db_contexts
    assert db_contexts["db1"] == context_string


def test_add_df_with_string_context() -> None:
    """Test adding a DataFrame with context as a string."""
    df = pd.DataFrame({"a": [1, 2, 3]})
    session = databao.open_session("ctx_string_session_df")
    context_string = "This is a string context for the DataFrame."
    session.add_df(df, context=context_string)

    _db_contexts, df_contexts = session.context
    assert "df1" in df_contexts
    assert df_contexts["df1"] == context_string


def test_add_additional_context_with_nonexistent_path_raises() -> None:
    """add_additional_context should raise if given a non-existent Path."""
    session = databao.open_session("additional_ctx_missing_path")
    with pytest.raises(FileNotFoundError):
        session.add_additional_context(Path("no_such_context_file_123.md"))


def test_add_additional_context_with_temp_file(temp_context_file: Path) -> None:
    """Ensure additional context can be loaded from a temporary file path."""
    session = databao.open_session("additional_ctx_from_file")
    session.add_additional_context(temp_context_file)
    assert session.additional_context == [temp_context_file.read_text()]


def test_add_additional_context_with_string() -> None:
    """Ensure additional context can be provided directly as a string."""
    session = databao.open_session("additional_ctx_from_string")
    text = "Global instructions for the session go here."
    session.add_additional_context(text)
    assert session.additional_context == [text]


def test_add_additional_context_multiple_calls_mixed_sources(temp_context_file: Path) -> None:
    """Calling add_additional_context multiple times should append in order."""
    session = databao.open_session("additional_ctx_multiple")
    first = "First global instruction."
    second = temp_context_file.read_text()
    third = "Third bit of context."

    session.add_additional_context(first)
    session.add_additional_context(temp_context_file)
    session.add_additional_context(third)

    assert first in session.additional_context
    assert second in session.additional_context
    assert third in session.additional_context
