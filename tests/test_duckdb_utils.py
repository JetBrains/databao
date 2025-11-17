import pytest
from sqlalchemy.engine.url import make_url

from databao.duckdb.utils import sqlalchemy_to_postgres_url


@pytest.mark.parametrize(
    ("input_url", "expected_output"),
    [
        (
            "postgresql://user:pass@localhost:5432/mydb",
            "postgresql://user:pass@localhost:5432/mydb",
        ),
        (
            "postgresql+psycopg2://user:pass@localhost:5432/mydb",
            "postgresql://user:pass@localhost:5432/mydb",
        ),
        (
            "postgresql+asyncpg://user:pass@localhost:5432/mydb",
            "postgresql://user:pass@localhost:5432/mydb",
        ),
        (
            "postgresql+psycopg2://user@localhost:5432/mydb",
            "postgresql://user@localhost:5432/mydb",
        ),
        (
            "postgresql://localhost:5432/mydb",
            "postgresql://localhost:5432/mydb",
        ),
        (
            "postgresql+psycopg2://user:pass@localhost/mydb",
            "postgresql://user:pass@localhost/mydb",
        ),
        (
            "postgresql+psycopg2://user:pass@localhost:5432/",
            "postgresql://user:pass@localhost:5432/",
        ),
        (
            "postgresql+psycopg2://postgres:qwe@localhost:5432/test",
            "postgresql://postgres:qwe@localhost:5432/test",
        ),
        (
            "postgresql://readonly_role:>sU9y95R(e4m@ep-young-breeze-a5cq8xns.us-east-2.aws.neon.tech/netflix?options=endpoint%3Dep-young-breeze-a5cq8xns&sslmode=require",
            "postgresql://readonly_role:%3EsU9y95R%28e4m@ep-young-breeze-a5cq8xns.us-east-2.aws.neon.tech/netflix?options=endpoint%3Dep-young-breeze-a5cq8xns&sslmode=require",
        ),
    ],
)
def test_sqlalchemy_to_duckdb_postgres(input_url: str, expected_output: str) -> None:
    url = make_url(input_url)
    result = sqlalchemy_to_postgres_url(url)
    assert result == expected_output
