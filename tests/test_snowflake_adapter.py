from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from databao_context_engine import (
    SnowflakeConnectionProperties,
    SnowflakeKeyPairAuth,
    SnowflakeOAuthAuth,
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

    assert params["auth_type"] == "ext_browser"
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
# _create_secret_params — OAuth token auth
# ---------------------------------------------------------------------------


def test_secret_params_oauth_token() -> None:
    auth = SnowflakeOAuthAuth(token="eyJhbGciOi.test.token")
    config = _make_config(auth)
    params = SnowflakeAdapter._create_secret_params(config)

    assert params["auth_type"] == "oauth"
    assert params["token"] == "eyJhbGciOi.test.token"
    assert "password" not in params


def test_secret_params_oauth_token_with_special_chars() -> None:
    auth = SnowflakeOAuthAuth(token="token'with'quotes")
    config = _make_config(auth)
    params = SnowflakeAdapter._create_secret_params(config)

    assert params["token"] == "token'with'quotes"


# ---------------------------------------------------------------------------
# _create_auth — OAuth token from content dict
# ---------------------------------------------------------------------------


def test_create_auth_recognizes_token() -> None:
    content = {**BASE_CONFIG, "token": "my_oauth_token"}
    auth = SnowflakeAdapter._create_auth(content)

    assert isinstance(auth, SnowflakeOAuthAuth)
    assert auth.token == "my_oauth_token"


# ---------------------------------------------------------------------------
# create_config_from_content — OAuth round-trip
# ---------------------------------------------------------------------------


def test_create_config_from_content_oauth() -> None:
    content = {
        "type": "snowflake",
        "connection": {
            **BASE_CONFIG,
            "auth": {"token": "my_oauth_token"},
        },
    }
    config = SnowflakeAdapter.create_config_from_content(content)

    assert isinstance(config, SnowflakeConnectionProperties)
    assert isinstance(config.auth, SnowflakeOAuthAuth)
    assert config.auth.token == "my_oauth_token"


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
            "autocommit": False,
            "user": "user",
            "password": "secret",
        }
    )
    config = SnowflakeAdapter.create_config_from_runtime(engine)
    assert isinstance(config, SnowflakeConnectionProperties)
    assert "host" not in config.additional_properties
    assert "port" not in config.additional_properties
    assert "autocommit" not in config.additional_properties


# ---------------------------------------------------------------------------
# create_sqlalchemy_engine — helpers
# ---------------------------------------------------------------------------


def _call_create_engine(config: SnowflakeConnectionProperties) -> tuple[dict[str, str], dict[str, Any]]:
    """Call create_sqlalchemy_engine with mocked URL and create_engine, returning (url_kwargs, connect_args)."""
    captured_url_kwargs: dict[str, str] = {}
    captured_connect_args: dict[str, Any] = {}

    def fake_url(**kwargs: str) -> str:
        captured_url_kwargs.update(kwargs)
        return "snowflake://fake"

    def fake_create_engine(url: Any, *, connect_args: dict[str, Any] | None = None) -> MagicMock:
        if connect_args:
            captured_connect_args.update(connect_args)
        return MagicMock()

    with (
        patch("databao.agent.databases.snowflake_adapter.create_engine", side_effect=fake_create_engine),
        patch.dict("sys.modules", {"snowflake": MagicMock(), "snowflake.sqlalchemy": MagicMock(URL=fake_url)}),
    ):
        result = SnowflakeAdapter.create_sqlalchemy_engine(config)

    assert result is not None
    return captured_url_kwargs, captured_connect_args


# ---------------------------------------------------------------------------
# create_sqlalchemy_engine — password auth
# ---------------------------------------------------------------------------


def test_create_engine_password_auth() -> None:
    config = _make_config(SnowflakePasswordAuth(password="s3cr3t"))
    url_kwargs, connect_args = _call_create_engine(config)

    assert url_kwargs["account"] == "myaccount"
    assert url_kwargs["user"] == "myuser"
    assert url_kwargs["database"] == "mydb"
    assert url_kwargs["warehouse"] == "mywh"
    assert url_kwargs["password"] == "s3cr3t"
    assert "private_key" not in connect_args
    assert "token" not in connect_args


def test_create_engine_password_auth_with_role() -> None:
    config = _make_config(SnowflakePasswordAuth(password="pw"), role="ANALYST")
    url_kwargs, _ = _call_create_engine(config)

    assert url_kwargs["role"] == "ANALYST"


def test_create_engine_password_auth_omits_none_fields() -> None:
    config = SnowflakeConnectionProperties(
        account="acct", user=None, database=None, warehouse=None, auth=SnowflakePasswordAuth(password="pw")
    )
    url_kwargs, _ = _call_create_engine(config)

    assert "user" not in url_kwargs
    assert "database" not in url_kwargs
    assert "warehouse" not in url_kwargs


# ---------------------------------------------------------------------------
# create_sqlalchemy_engine — key pair auth
# ---------------------------------------------------------------------------


def test_create_engine_key_pair_auth(tmp_path: Path) -> None:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    key_file = tmp_path / "rsa_key.pem"
    key_file.write_bytes(pem)

    auth = SnowflakeKeyPairAuth(private_key_file=str(key_file))
    config = _make_config(auth)
    url_kwargs, connect_args = _call_create_engine(config)

    assert "password" not in url_kwargs
    assert "private_key" in connect_args
    assert isinstance(connect_args["private_key"], bytes)


def test_create_engine_key_pair_auth_bad_file_raises() -> None:
    auth = SnowflakeKeyPairAuth(private_key_file="/nonexistent/key.pem")
    config = _make_config(auth)

    with pytest.raises(ValueError, match="Failed to read private key file"):
        _call_create_engine(config)


# ---------------------------------------------------------------------------
# create_sqlalchemy_engine — OAuth auth
# ---------------------------------------------------------------------------


def test_create_engine_oauth_auth() -> None:
    auth = SnowflakeOAuthAuth(token="eyJhbGciOi.test.token")
    config = _make_config(auth)
    url_kwargs, connect_args = _call_create_engine(config)

    assert "password" not in url_kwargs
    assert connect_args["authenticator"] == "oauth"
    assert connect_args["token"] == "eyJhbGciOi.test.token"


# ---------------------------------------------------------------------------
# create_sqlalchemy_engine — SSO auth
# ---------------------------------------------------------------------------


def test_create_engine_sso_externalbrowser() -> None:
    auth = SnowflakeSSOAuth(authenticator="externalbrowser")
    config = _make_config(auth)
    url_kwargs, connect_args = _call_create_engine(config)

    assert url_kwargs["authenticator"] == "externalbrowser"
    assert "token" not in connect_args


def test_create_engine_sso_okta() -> None:
    auth = SnowflakeSSOAuth(authenticator="https://myorg.okta.com")
    config = _make_config(auth)
    url_kwargs, _ = _call_create_engine(config)

    assert url_kwargs["authenticator"] == "https://myorg.okta.com"


# ---------------------------------------------------------------------------
# create_sqlalchemy_engine — additional_properties
# ---------------------------------------------------------------------------


def test_create_engine_includes_additional_properties() -> None:
    config = _make_config(
        SnowflakePasswordAuth(password="pw"),
        additional_properties={"timeout": 30, "client_session_keep_alive": True},
    )
    _, connect_args = _call_create_engine(config)

    assert connect_args["timeout"] == 30
    assert connect_args["client_session_keep_alive"] is True


# ---------------------------------------------------------------------------
# create_sqlalchemy_engine — unsupported config
# ---------------------------------------------------------------------------


def test_create_engine_returns_none_for_non_snowflake_config() -> None:
    result = SnowflakeAdapter.create_sqlalchemy_engine(MagicMock())
    assert result is None


# ---------------------------------------------------------------------------
# create_config_from_runtime — TOKEN_KEY excluded from additional_properties
# ---------------------------------------------------------------------------


def test_create_config_from_runtime_excludes_token_from_additional_properties() -> None:
    """TOKEN_KEY must be in EXCLUDED_QUERY_KEYS so OAuth tokens don't leak into additional_properties."""
    engine = _make_snowflake_engine(
        {
            "account": "acct",
            "host": "acct.snowflakecomputing.com",
            "user": "user",
            "token": "secret-oauth-token",
        }
    )
    config = SnowflakeAdapter.create_config_from_runtime(engine)
    assert isinstance(config, SnowflakeConnectionProperties)
    assert "token" not in config.additional_properties
    # The token should be captured in the auth object
    assert isinstance(config.auth, SnowflakeOAuthAuth)
    assert config.auth.token == "secret-oauth-token"
