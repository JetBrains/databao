import pytest

from portus.data.database_type_utils import is_id_column


@pytest.mark.parametrize(
    "col_name, expected",
    [
        ("user_id", True),
        ("transaction_id", True),
        ("id", True),
        ("code", True),
        ("product_code", True),
        ("description", False),
        ("12345", False),
        ("id_code", True),
        ("@!#($*&", False),
    ],
)
def test_is_id_column(col_name: str, expected: bool) -> None:
    assert expected == is_id_column(col_name)
