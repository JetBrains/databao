from pathlib import Path
from typing import Any

from databao_context_engine import (
    SnowflakeConnectionProperties,
    SnowflakeKeyPairAuth,
    SnowflakePasswordAuth,
    SnowflakeSSOAuth,
)

from databao.databases.snowflake_adapter import SnowflakeAdapter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BASE_CONFIG: dict[str, Any] = dict(account="myaccount", user="myuser", database="mydb", warehouse="mywh")


def _make_config(auth: Any, **kwargs: Any) -> SnowflakeConnectionProperties:
    return SnowflakeConnectionProperties(**{**BASE_CONFIG, **kwargs}, auth=auth)


def _parse_secret_sql(sql: str) -> dict[str, str]:
    """Parse 'CREATE OR REPLACE SECRET "name" (TYPE snowflake, k 'v', ...)' into a dict."""
    # Extract the key-value section between the outer parentheses
    inner = sql[sql.index("(") + 1 : sql.rindex(")")]
    # Drop the leading "TYPE snowflake, " prefix
    inner = inner.split(", ", 1)[1]
    result: dict[str, str] = {}
    for part in inner.split(", "):
        key, _, quoted_value = part.partition(" ")
        result[key] = quoted_value.strip("'")
    return result


# ---------------------------------------------------------------------------
# _create_secret_sql — password auth
# ---------------------------------------------------------------------------


def test_secret_sql_password_auth_structure() -> None:
    config = _make_config(SnowflakePasswordAuth(password="s3cr3t"))
    sql = SnowflakeAdapter._create_secret_sql(config, "mydb")

    assert sql.startswith('CREATE OR REPLACE SECRET "mydb" (TYPE snowflake,')
    assert sql.endswith(");")


def test_secret_sql_password_auth_params() -> None:
    config = _make_config(SnowflakePasswordAuth(password="s3cr3t"))
    params = _parse_secret_sql(SnowflakeAdapter._create_secret_sql(config, "mydb"))

    assert params["account"] == "myaccount"
    assert params["user"] == "myuser"
    assert params["database"] == "mydb"
    assert params["warehouse"] == "mywh"
    assert params["password"] == "s3cr3t"
    assert "auth_type" not in params


def test_secret_sql_password_auth_no_role_by_default() -> None:
    config = _make_config(SnowflakePasswordAuth(password="s3cr3t"))
    params = _parse_secret_sql(SnowflakeAdapter._create_secret_sql(config, "mydb"))
    assert "role" not in params


def test_secret_sql_password_auth_includes_role_when_set() -> None:
    config = _make_config(SnowflakePasswordAuth(password="s3cr3t"), role="ANALYST")
    params = _parse_secret_sql(SnowflakeAdapter._create_secret_sql(config, "mydb"))
    assert params["role"] == "ANALYST"


def test_secret_sql_omits_database_when_none() -> None:
    config = SnowflakeConnectionProperties(
        account="acct", user="usr", database=None, warehouse="wh", auth=SnowflakePasswordAuth(password="pw")
    )
    params = _parse_secret_sql(SnowflakeAdapter._create_secret_sql(config, "s"))
    assert "database" not in params


def test_secret_sql_omits_warehouse_when_none() -> None:
    config = SnowflakeConnectionProperties(
        account="acct", user="usr", database="db", warehouse=None, auth=SnowflakePasswordAuth(password="pw")
    )
    params = _parse_secret_sql(SnowflakeAdapter._create_secret_sql(config, "s"))
    assert "warehouse" not in params


# ---------------------------------------------------------------------------
# _create_secret_sql — key pair auth (inline key)
# ---------------------------------------------------------------------------


def test_secret_sql_key_pair_inline_key() -> None:
    auth = SnowflakeKeyPairAuth(private_key="-----BEGIN PRIVATE KEY-----\nABC\n-----END PRIVATE KEY-----\n")
    config = _make_config(auth)
    params = _parse_secret_sql(SnowflakeAdapter._create_secret_sql(config, "mydb"))

    assert params["auth_type"] == "key_pair"
    assert "BEGIN PRIVATE KEY" in params["private_key"]
    assert "password" not in params
    assert "private_key_passphrase" not in params


def test_secret_sql_key_pair_inline_key_with_passphrase() -> None:
    auth = SnowflakeKeyPairAuth(
        private_key="-----BEGIN ENCRYPTED PRIVATE KEY-----\nXYZ\n-----END ENCRYPTED PRIVATE KEY-----\n",
        private_key_file_pwd="mypassphrase",
    )
    config = _make_config(auth)
    params = _parse_secret_sql(SnowflakeAdapter._create_secret_sql(config, "mydb"))

    assert params["auth_type"] == "key_pair"
    assert params["private_key_passphrase"] == "mypassphrase"


# ---------------------------------------------------------------------------
# _create_secret_sql — key pair auth (file path)
# ---------------------------------------------------------------------------


def test_secret_sql_key_pair_file_reads_content(tmp_path: Path) -> None:
    key_content = "-----BEGIN PRIVATE KEY-----\nFILE_KEY\n-----END PRIVATE KEY-----\n"
    key_file = tmp_path / "rsa_key.p8"
    key_file.write_text(key_content)

    auth = SnowflakeKeyPairAuth(private_key_file=str(key_file))
    config = _make_config(auth)
    params = _parse_secret_sql(SnowflakeAdapter._create_secret_sql(config, "mydb"))

    assert params["auth_type"] == "key_pair"
    assert params["private_key"] == key_content


def test_secret_sql_key_pair_file_with_passphrase(tmp_path: Path) -> None:
    key_file = tmp_path / "rsa_key.p8"
    key_file.write_text("key")

    auth = SnowflakeKeyPairAuth(private_key_file=str(key_file), private_key_file_pwd="phrase")
    config = _make_config(auth)
    params = _parse_secret_sql(SnowflakeAdapter._create_secret_sql(config, "mydb"))

    assert params["private_key_passphrase"] == "phrase"


# ---------------------------------------------------------------------------
# _create_secret_sql — SSO auth
# ---------------------------------------------------------------------------


def test_secret_sql_sso_externalbrowser() -> None:
    auth = SnowflakeSSOAuth(authenticator="externalbrowser")
    config = _make_config(auth)
    params = _parse_secret_sql(SnowflakeAdapter._create_secret_sql(config, "mydb"))

    assert params["auth_type"] == "externalbrowser"
    assert "okta_url" not in params
    assert "password" not in params


def test_secret_sql_sso_okta_url() -> None:
    okta_url = "https://myorg.okta.com"
    auth = SnowflakeSSOAuth(authenticator=okta_url)
    config = _make_config(auth)
    params = _parse_secret_sql(SnowflakeAdapter._create_secret_sql(config, "mydb"))

    assert params["auth_type"] == "okta"
    assert params["okta_url"] == okta_url


def test_secret_sql_sso_oauth() -> None:
    auth = SnowflakeSSOAuth(authenticator="oauth")
    config = _make_config(auth)
    params = _parse_secret_sql(SnowflakeAdapter._create_secret_sql(config, "mydb"))

    assert params["auth_type"] == "oauth"


# ---------------------------------------------------------------------------
# _create_secret_sql — secret name quoting
# ---------------------------------------------------------------------------


def test_secret_sql_name_used_correctly() -> None:
    config = _make_config(SnowflakePasswordAuth(password="pw"))
    sql = SnowflakeAdapter._create_secret_sql(config, "my_secret_name")
    assert 'SECRET "my_secret_name"' in sql
