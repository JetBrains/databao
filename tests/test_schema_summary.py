import pytest

from portus.data.schema_summary import normalize_dtype


@pytest.mark.parametrize(
    "dtype, expected_dtype",
    [
        ("int", "int"),
        ('VARCHAR(50) COLLATE ""SQL_Latin1_General_CP1_CI_AS""', "VARCHAR(50)"),
        ("VARCHAR(50)", "VARCHAR(50)"),
    ],
)
def test_normalize_dtype(dtype: str, expected_dtype: str) -> None:
    assert normalize_dtype(dtype) == expected_dtype
