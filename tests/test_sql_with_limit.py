import pytest

from databao.duckdb.react_tools import sql_with_limit


@pytest.mark.parametrize(
    "sql,limit,expected",
    [
        ("SELECT 1", 5, "SELECT 1 LIMIT 5"),
        ("SELECT * FROM t", 10, "SELECT * FROM t LIMIT 10"),
        ("SELECT 1;", 3, "SELECT 1 LIMIT 3"),
        ("\n  SELECT 1  \n", 2, "SELECT 1 LIMIT 2"),
        ("SELECT * FROM (select abc from foo limit 5)", 7, "SELECT * FROM (select abc from foo limit 5) LIMIT 7"),
        (
            'SELECT "limit" FROM (select "limit" from foo limit 5)',
            7,
            'SELECT "limit" FROM (select "limit" from foo limit 5) limit 7',
        ),
        ('SELECT "limit" FROM (select "limit" from foo)', 7, 'SELECT "limit" FROM (select "limit" from foo) limit 7'),
    ],
)
def test_sql_with_limit_appends_when_missing(sql: str, limit: int, expected: str) -> None:
    assert sql_with_limit(sql, limit).lower() == expected.lower()


@pytest.mark.parametrize(
    "sql_with_existing_limit",
    [
        "SELECT 1 LIMIT 1",
        "select 1 limit 2",
        "SELECT * FROM t WHERE x = 1 LIMIT 100;",
        "  SELECT * FROM t\nLIMIT 7  ",
        "  SELECT * FROM t\n  LIMIT 7  ",
        "  SELECT * FROM (select * from foo limit 5)\n  LIMIT 7",
        "SELECT 1 LIMIT ?",
        "SELECT 1 LIMIT :param",
        "SELECT 1 LIMIT $1",
        "SELECT 1 LIMIT @var",
    ],
)
def test_sql_with_limit_does_not_duplicate_when_present(sql_with_existing_limit: str) -> None:
    out = sql_with_limit(sql_with_existing_limit, 999)
    expected = sql_with_existing_limit.strip().rstrip(";")  # No other changes
    assert out.lower() == expected.lower()
