import logging

import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

import portus


@pytest.fixture
def db_engine() -> Engine:
    """Create database engine for testing."""
    engine = create_engine(
        "postgresql://readonly_role:>sU9y95R(e4m@ep-young-breeze-a5cq8xns.us-east-2.aws.neon.tech/netflix?options=endpoint%3Dep-young-breeze-a5cq8xns&sslmode=require"
    )
    return engine


def test_demo_smoke(db_engine: Engine) -> None:
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

    # Step 2: Create portus session
    session = portus.open_session("test_session")
    assert session is not None

    # Step 3: Add database to session
    session.add_db(db_engine)

    # Step 4: Create and add DataFrame to session
    data = {"show_id": ["s706", "s1032", "s1253"], "cancelled": [True, True, False]}
    df = pd.DataFrame(data)
    session.add_df(df)

    # Step 5: Ask a question and get results
    ask = session.ask("count cancelled shows by directors")
    assert ask is not None

    # Step 6: Get DataFrame result
    result_df = ask.df()
    assert result_df is not None

    # Step 7: Generate plot
    plot = ask.plot()
    assert plot.plot is not None

    # Step 8: Verify code is generated
    assert ask.code is not None
    assert len(ask.code) > 0, "Expected generated code to be non-empty"


def test_consecutive_ask_calls(db_engine: Engine) -> None:
    """Test consecutive ask calls return different results (marko.py variation)."""
    # Configure logging
    logging.basicConfig(level=logging.INFO)

    # Step 1: Create portus session
    session = portus.open_session("test_consecutive_session")
    assert session is not None

    # Step 2: Add database to session
    session.add_db(db_engine)

    # Step 3: Create and add DataFrame to session
    data = {"show_id": ["s706", "s1032", "s1253"], "cancelled": [True, True, False]}
    df = pd.DataFrame(data)
    session.add_df(df)

    # Step 4: First ask - count cancelled shows by directors
    ask1 = session.ask("count cancelled shows by directors")
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
