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
from databao.core.domain import Domain


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
def domain() -> Domain:
    return databao.domain()


def _new_agent(domain: Domain) -> Agent:
    llm_config = LLMConfigDirectory.DEFAULT.model_copy(update={"model_kwargs": {"api_key": "test"}})
    return databao.agent(domain, llm_config=llm_config)


def test_add_db_with_nonexistent_context_path_raises(domain: Domain, duckdb_conn: DuckDBPyConnection) -> None:
    with pytest.raises(FileNotFoundError):
        domain.add_db(duckdb_conn, description=Path("this_file_does_not_exist_123456.md"))


def test_add_df_with_nonexistent_context_path_raises(domain: Domain) -> None:
    df = pd.DataFrame({"a": [1, 2, 3]})

    with pytest.raises(FileNotFoundError):
        domain.add_df(df, description=Path("another_missing_context_987654.md"))


def test_add_db_with_temp_file_context(
    domain: Domain, temp_context_file: Path, duckdb_conn: DuckDBPyConnection
) -> None:
    """Test adding a database with context from a temporary file."""
    domain.add_db(duckdb_conn, description=temp_context_file)
    agent = _new_agent(domain)

    assert "db1" in agent.dbs
    assert agent.dbs["db1"].description == temp_context_file.read_text()


def test_add_df_with_temp_file_context(domain: Domain, temp_context_file: Path) -> None:
    """Test adding a DataFrame with context from a temporary file."""
    df = pd.DataFrame({"a": [1, 2, 3]})

    domain.add_df(df, description=temp_context_file)
    agent = _new_agent(domain)

    assert "df1" in agent.dfs
    assert agent.dfs["df1"].description == temp_context_file.read_text()


def test_add_db_with_string_context(domain: Domain, duckdb_conn: DuckDBPyConnection) -> None:
    """Test adding a database with context as a string."""
    context_string = "This is a string context for the database."

    domain.add_db(duckdb_conn, description=context_string)
    agent = _new_agent(domain)

    assert "db1" in agent.dbs
    assert agent.dbs["db1"].description == context_string


def test_add_df_with_string_context(domain: Domain) -> None:
    """Test adding a DataFrame with context as a string."""
    df = pd.DataFrame({"a": [1, 2, 3]})
    context_string = "This is a string context for the DataFrame."

    domain.add_df(df, description=context_string)
    agent = _new_agent(domain)

    assert "df1" in agent.dfs
    assert agent.dfs["df1"].description == context_string


def test_add_additional_context_with_nonexistent_path_raises(domain: Domain) -> None:
    """add_additional_context should raise if given a non-existent Path."""
    with pytest.raises(FileNotFoundError):
        domain.add_description(Path("no_such_context_file_123.md"))


def test_add_additional_context_with_temp_file(domain: Domain, temp_context_file: Path) -> None:
    """Ensure additional context can be loaded from a temporary file path."""
    domain.add_description(temp_context_file)
    agent = _new_agent(domain)

    assert agent.additional_description == [temp_context_file.read_text()]


def test_add_additional_context_with_string(domain: Domain) -> None:
    """Ensure additional context can be provided directly as a string."""
    text = "Global instructions for the agent go here."

    domain.add_description(text)
    agent = _new_agent(domain)

    assert agent.additional_description == [text]


def test_add_additional_context_multiple_calls_mixed_sources(domain: Domain, temp_context_file: Path) -> None:
    """Calling add_additional_context multiple times should append in order."""
    first = "First global instruction."
    second = temp_context_file.read_text()
    third = "Third bit of context."

    domain.add_description(first)
    domain.add_description(temp_context_file)
    domain.add_description(third)
    agent = _new_agent(domain)

    assert first in agent.additional_description
    assert second in agent.additional_description
    assert third in agent.additional_description
