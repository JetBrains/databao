import logging
import shutil
import uuid
from collections.abc import Generator
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

import databao
from databao import LLMConfig
from databao.core.context import Context, ContextBuilder


@pytest.fixture(params=["static", "dynamic"])
def builder(request: pytest.FixtureRequest) -> Generator[ContextBuilder, None, None]:
    mode = request.param

    if mode == "dynamic":
        yield Context.builder()
        return

    root = Path(request.config.rootpath)
    base = root / "tests/.pytest-artifacts"
    base.mkdir(parents=True, exist_ok=True)

    path = base / f"context-{request.node.name}-{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=False)

    builder = Context.builder(path)
    yield builder

    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def db_engine() -> Engine:
    """Create database engine for testing."""
    engine = create_engine(
        "postgresql://readonly_role:>sU9y95R(e4m@ep-young-breeze-a5cq8xns.us-east-2.aws.neon.tech/netflix?options=endpoint%3Dep-young-breeze-a5cq8xns&sslmode=require"
    )
    return engine


@pytest.mark.apikey
def test_demo_smoke(builder: ContextBuilder, db_engine: Engine) -> None:
    """Smoke test to ensure demo.py steps execute without exceptions."""
    # Configure logging
    logging.basicConfig(level=logging.INFO)

    # Step 1: Read data from database
    df = pd.read_sql(
        """
        SELECT *
        FROM netflix_shows
        WHERE country = 'Germany'
        """,
        db_engine,
    )
    assert df is not None
    assert len(df) > 0, "Expected to get some results from the database query"

    # Step 2: Add database to context
    builder.add_db(db_engine)

    # Step 3: Create and add DataFrame to context
    data = {"show_id": ["s706", "s1032", "s1253"], "cancelled": [True, True, False]}
    df = pd.DataFrame(data)
    builder.add_df(df)

    # Step 4: Builder a context and create a databao agent
    context = builder.build()
    agent = databao.agent(context, "test_agent", LLMConfig(name="gpt-5"))
    assert agent is not None

    # Step 5: Ask a question and get results
    ask = agent.thread().ask("count cancelled shows by directors")
    assert ask is not None

    # Step 6: Get DataFrame result
    result_df = ask.df()
    assert result_df is not None

    # Step 7: Generate plot
    plot = ask.plot()
    assert plot.plot is not None

    # Step 8: Verify code is generated
    code = ask.code()
    assert code is not None
    assert len(code) > 0, "Expected generated code to be non-empty"


@pytest.mark.apikey
def test_consecutive_ask_calls(builder: ContextBuilder, db_engine: Engine) -> None:
    """Test consecutive ask calls return different results."""
    # Configure logging
    logging.basicConfig(level=logging.INFO)

    # Step 1: Add database to context
    builder.add_db(db_engine)

    # Step 2: Create and add DataFrame to context
    data = {"show_id": ["s706", "s1032", "s1253"], "cancelled": [True, True, False]}
    df = pd.DataFrame(data)
    builder.add_df(df)

    # Step 3: Builder a context and create a databao agent
    context = builder.build()
    agent = databao.agent(context, "test_consecutive_agent", LLMConfig(name="gpt-5"))
    assert agent is not None

    # Step 4: First ask - count cancelled shows by directors
    ask1 = agent.thread().ask("count cancelled shows by directors")
    assert ask1 is not None

    # Step 5: Get text result from first ask
    result1 = ask1.text()
    assert result1 is not None
    assert len(result1) > 0, "Expected first ask to return non-empty text result"

    # Step 6: Second ask (chained) - give me just their names
    ask2 = ask1.ask("give me just their names")
    assert ask2 is not None

    # Step 7: Get text result from second ask
    result2 = ask2.text()
    assert result2 is not None
    assert len(result2) > 0, "Expected second ask to return non-empty text result"

    # Step 8: Verify that consecutive calls return different results
    assert result1 != result2, (
        "Expected consecutive ask calls to return different results. "
        f"First result: {result1[:100]}... "
        f"Second result: {result2[:100]}..."
    )
