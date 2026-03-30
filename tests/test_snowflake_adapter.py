from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from databao_context_engine import (
    SnowflakeConnectionProperties,
    SnowflakeKeyPairAuth,
    SnowflakePasswordAuth,
    SnowflakeSSOAuth,
)

from databao.agent.databases.snowflake_adapter import SnowflakeAdapter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BASE_CONFIG: dict[str, Any] = dict(account="myaccount", user="myuser", database="mydb", warehouse="mywh")


def _make_config(auth: Any, **kwargs: Any) -> SnowflakeConnectionProperties:
    return SnowflakeConnectionProperties(**{**BASE_CONFIG, **kwargs}, auth=auth)


# ---------------------------------------------------------------------------
# _create_secret_params — password auth
# ---------------------------------------------------------------------------


def test_secret_params_password_auth() -> None:
    config = _make_config(SnowflakePasswordAuth(password="s3cr3t"))
    params = SnowflakeAdapter._create_secret_params(config)

    assert params["account"] == "myaccount"
    assert params["user"] == "myuser"
    assert params["database"] == "mydb"
    assert params["warehouse"] == "mywh"
    assert params["password"] == "s3cr3t"
    assert "auth_type" not in params


def test_secret_params_password_auth_no_role_by_default() -> None:
    config = _make_config(SnowflakePasswordAuth(password="s3cr3t"))
    params = SnowflakeAdapter._create_secret_params(config)
    assert "role" not in params


def test_secret_params_password_auth_includes_role_when_set() -> None:
    config = _make_config(SnowflakePasswordAuth(password="s3cr3t"), role="ANALYST")
    params = SnowflakeAdapter._create_secret_params(config)
    assert params["role"] == "ANALYST"


def test_secret_params_omits_database_when_none() -> None:
    config = SnowflakeConnectionProperties(
        account="acct", user="usr", database=None, warehouse="wh", auth=SnowflakePasswordAuth(password="pw")
    )
    params = SnowflakeAdapter._create_secret_params(config)
    assert "database" not in params


def test_secret_params_omits_warehouse_when_none() -> None:
    config = SnowflakeConnectionProperties(
        account="acct", user="usr", database="db", warehouse=None, auth=SnowflakePasswordAuth(password="pw")
    )
    params = SnowflakeAdapter._create_secret_params(config)
    assert "warehouse" not in params


# ---------------------------------------------------------------------------
# _create_secret_params — key pair auth (inline key)
# ---------------------------------------------------------------------------


def test_secret_params_key_pair_inline_key() -> None:
    auth = SnowflakeKeyPairAuth(private_key="-----BEGIN PRIVATE KEY-----\nABC\n-----END PRIVATE KEY-----\n")
    config = _make_config(auth)
    params = SnowflakeAdapter._create_secret_params(config)

    assert params["auth_type"] == "key_pair"
    assert "BEGIN PRIVATE KEY" in params["private_key"]
    assert "password" not in params
    assert "private_key_passphrase" not in params


def test_secret_params_key_pair_inline_key_with_passphrase() -> None:
    auth = SnowflakeKeyPairAuth(
        private_key="-----BEGIN ENCRYPTED PRIVATE KEY-----\nXYZ\n-----END ENCRYPTED PRIVATE KEY-----\n",
        private_key_file_pwd="mypassphrase",
    )
    config = _make_config(auth)
    params = SnowflakeAdapter._create_secret_params(config)

    assert params["auth_type"] == "key_pair"
    assert params["private_key_passphrase"] == "mypassphrase"


# ---------------------------------------------------------------------------
# _create_secret_params — key pair auth (file path)
# ---------------------------------------------------------------------------


def test_secret_params_key_pair_file_reads_content(tmp_path: Path) -> None:
    key_content = "-----BEGIN PRIVATE KEY-----\nFILE_KEY\n-----END PRIVATE KEY-----\n"
    key_file = tmp_path / "rsa_key.p8"
    key_file.write_text(key_content)

    auth = SnowflakeKeyPairAuth(private_key_file=str(key_file))
    config = _make_config(auth)
    params = SnowflakeAdapter._create_secret_params(config)

    assert params["auth_type"] == "key_pair"
    assert params["private_key"] == key_content


def test_secret_params_key_pair_file_with_passphrase(tmp_path: Path) -> None:
    key_file = tmp_path / "rsa_key.p8"
    key_file.write_text("key")

    auth = SnowflakeKeyPairAuth(private_key_file=str(key_file), private_key_file_pwd="phrase")
    config = _make_config(auth)
    params = SnowflakeAdapter._create_secret_params(config)

    assert params["private_key_passphrase"] == "phrase"


# ---------------------------------------------------------------------------
# _create_secret_params — SSO auth
# ---------------------------------------------------------------------------


def test_secret_params_sso_externalbrowser() -> None:
    auth = SnowflakeSSOAuth(authenticator="externalbrowser")
    config = _make_config(auth)
    params = SnowflakeAdapter._create_secret_params(config)

    assert params["auth_type"] == "externalbrowser"
    assert "okta_url" not in params
    assert "password" not in params


def test_secret_params_sso_okta_url() -> None:
    okta_url = "https://myorg.okta.com"
    auth = SnowflakeSSOAuth(authenticator=okta_url)
    config = _make_config(auth)
    params = SnowflakeAdapter._create_secret_params(config)

    assert params["auth_type"] == "okta"
    assert params["okta_url"] == okta_url


def test_secret_params_sso_oauth() -> None:
    auth = SnowflakeSSOAuth(authenticator="oauth")
    config = _make_config(auth)
    params = SnowflakeAdapter._create_secret_params(config)

    assert params["auth_type"] == "oauth"


# ---------------------------------------------------------------------------
# _create_secret_params — values with special characters
# ---------------------------------------------------------------------------


def test_secret_params_preserves_single_quotes_in_password() -> None:
    config = _make_config(SnowflakePasswordAuth(password="my'password"))
    params = SnowflakeAdapter._create_secret_params(config)
    assert params["password"] == "my'password"


def test_secret_params_includes_additional_properties() -> None:
    config = _make_config(SnowflakePasswordAuth(password="pw"), additional_properties={"timeout": 30, "custom": "val"})
    params = SnowflakeAdapter._create_secret_params(config)
    assert params["timeout"] == "30"
    assert params["custom"] == "val"


# ---------------------------------------------------------------------------
# _format_sql_params — SQL formatting and escaping
# ---------------------------------------------------------------------------


def test_format_sql_params_basic() -> None:
    assert SnowflakeAdapter._format_sql_params({"account": "acct", "user": "me"}) == "account 'acct', user 'me'"


def test_format_sql_params_escapes_single_quotes() -> None:
    assert SnowflakeAdapter._format_sql_params({"password": "my'pass"}) == "password 'my''pass'"


# ---------------------------------------------------------------------------
# _create_secret_params — error handling
# ---------------------------------------------------------------------------


def test_secret_params_key_pair_no_key_raises() -> None:
    auth = SnowflakeKeyPairAuth(private_key=None, private_key_file=None)
    config = _make_config(auth)
    with pytest.raises(ValueError, match="No private key provided"):
        SnowflakeAdapter._create_secret_params(config)


def test_secret_params_key_pair_file_not_found_raises() -> None:
    auth = SnowflakeKeyPairAuth(private_key_file="/nonexistent/path/key.p8")
    config = _make_config(auth)
    with pytest.raises(ValueError, match="Unable to read Snowflake private key file"):
        SnowflakeAdapter._create_secret_params(config)


# ---------------------------------------------------------------------------
# register_in_duckdb — statement ordering
# ---------------------------------------------------------------------------


def test_register_in_duckdb_executes_statements_in_order() -> None:
    config = _make_config(SnowflakePasswordAuth(password="s3cr3t"))
    conn = MagicMock()

    SnowflakeAdapter.register_in_duckdb(conn, config, "mydb")

    calls = [c.args[0] for c in conn.execute.call_args_list]
    assert len(calls) == 4
    assert calls[0] == "INSTALL snowflake FROM community;"
    assert calls[1] == "LOAD snowflake;"
    assert calls[2].startswith('CREATE OR REPLACE SECRET "mydb" (TYPE snowflake,')
    assert calls[3] == """ATTACH '' AS "mydb" (TYPE snowflake, SECRET "mydb", READ_ONLY);"""


# ---------------------------------------------------------------------------
# create_config_from_runtime — account / region reconstruction
# ---------------------------------------------------------------------------


def _make_snowflake_engine(connect_args: dict[str, Any]) -> MagicMock:
    from sqlalchemy import Engine

    mock = MagicMock()
    mock.__class__ = Engine  # type: ignore[assignment]
    mock.dialect.name = "snowflake"
    mock.url.render_as_string.return_value = "snowflake://user:pass@account/db"
    mock.dialect.create_connect_args.return_value = ([], connect_args)
    return mock


def test_create_config_from_runtime_preserves_region_in_account() -> None:
    engine = _make_snowflake_engine(
        {
            "account": "nameaccount",
            "host": "nameaccount.eu-central-1.snowflakecomputing.com",
            "user": "user@example.com",
            "dbname": "MYDB",
            "warehouse": "WH",
            "password": "secret",
        }
    )
    config = SnowflakeAdapter.create_config_from_runtime(engine)
    assert isinstance(config, SnowflakeConnectionProperties)
    assert config.account == "nameaccount.eu-central-1"


def test_create_config_from_runtime_no_region_keeps_bare_account() -> None:
    engine = _make_snowflake_engine(
        {
            "account": "nameaccount",
            "host": "nameaccount.snowflakecomputing.com",
            "user": "user",
            "password": "secret",
        }
    )
    config = SnowflakeAdapter.create_config_from_runtime(engine)
    assert isinstance(config, SnowflakeConnectionProperties)
    assert config.account == "nameaccount"


def test_create_config_from_runtime_no_host_falls_back_to_account() -> None:
    engine = _make_snowflake_engine(
        {
            "account": "nameaccount",
            "user": "user",
            "password": "secret",
        }
    )
    config = SnowflakeAdapter.create_config_from_runtime(engine)
    assert isinstance(config, SnowflakeConnectionProperties)
    assert config.account == "nameaccount"


def test_create_config_from_runtime_host_not_in_additional_properties() -> None:
    engine = _make_snowflake_engine(
        {
            "account": "nameaccount",
            "host": "nameaccount.eu-central-1.snowflakecomputing.com",
            "port": "443",
            "user": "user",
            "password": "secret",
        }
    )
    config = SnowflakeAdapter.create_config_from_runtime(engine)
    assert isinstance(config, SnowflakeConnectionProperties)
    assert "host" not in config.additional_properties
