"""Default browser-based OAuth flow for MCP servers."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import socket
import threading
import webbrowser
from collections.abc import Awaitable, Callable
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx
from mcp.client.auth import OAuthClientProvider, TokenStorage
from mcp.shared.auth import OAuthClientInformationFull, OAuthToken

logger = logging.getLogger(__name__)

_TOKEN_DIR = (
    Path(os.environ["DATABAO_MCP_TOKEN_DIR"])
    if "DATABAO_MCP_TOKEN_DIR" in os.environ
    else Path.home() / ".databao" / "mcp-tokens"
)
_DEFAULT_CALLBACK_PORT_RANGE = (18400, 18500)
_DEFAULT_TIMEOUT = 300.0


# ---------------------------------------------------------------------------
# File-based token storage
# ---------------------------------------------------------------------------


class FileTokenStorage:
    """Persist OAuth tokens and client registration to disk."""

    def __init__(self, server_url: str) -> None:
        url_hash = hashlib.sha256(server_url.encode()).hexdigest()[:16]
        self._dir = _TOKEN_DIR / url_hash
        self._dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._tokens_path = self._dir / "tokens.json"
        self._client_path = self._dir / "client.json"

    async def get_tokens(self) -> OAuthToken | None:
        return self._read_model(self._tokens_path, OAuthToken)

    async def set_tokens(self, tokens: OAuthToken) -> None:
        self._write_json(self._tokens_path, tokens.model_dump(mode="json", exclude_none=True))

    async def get_client_info(self) -> OAuthClientInformationFull | None:
        return self._read_model(self._client_path, OAuthClientInformationFull)

    async def set_client_info(self, client_info: OAuthClientInformationFull) -> None:
        self._write_json(self._client_path, client_info.model_dump(mode="json", exclude_none=True))

    @staticmethod
    def _read_model(path: Path, model_cls: type[Any]) -> Any | None:
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return model_cls.model_validate(data)
        except Exception:
            logger.debug("Failed to read %s", path, exc_info=True)
            return None

    @staticmethod
    def _write_json(path: Path, data: Any) -> None:
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        try:
            path.chmod(0o600)
        except (NotImplementedError, OSError):
            logger.debug("Could not set permissions on %s", path)


# ---------------------------------------------------------------------------
# Local callback server
# ---------------------------------------------------------------------------


def _find_free_port(lo: int = _DEFAULT_CALLBACK_PORT_RANGE[0], hi: int = _DEFAULT_CALLBACK_PORT_RANGE[1]) -> int:
    for port in range(lo, hi):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    raise RuntimeError(f"No free port in range {lo}-{hi} for OAuth callback")


class _CallbackHandler(BaseHTTPRequestHandler):
    """Handles a single OAuth redirect request."""

    code: str | None = None
    state: str | None = None
    _event: threading.Event

    def do_GET(self) -> None:
        qs = parse_qs(urlparse(self.path).query)
        codes = qs.get("code", [])
        states = qs.get("state", [])
        _CallbackHandler.code = codes[0] if codes else None
        _CallbackHandler.state = states[0] if states else None

        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(b"<html><body><h2>Authorization successful.</h2><p>You can close this tab.</p></body></html>")
        self._event.set()

    def log_message(self, format: str, *args: Any) -> None:
        logger.debug(format, *args)


# ---------------------------------------------------------------------------
# Redirect + callback handlers
# ---------------------------------------------------------------------------


def _make_handlers(
    port: int,
) -> tuple[
    Callable[[str], Awaitable[None]],
    Callable[[], Awaitable[tuple[str, str | None]]],
]:
    """Build the async redirect_handler and callback_handler for OAuthClientProvider."""
    event = threading.Event()
    _CallbackHandler._event = event
    _CallbackHandler.code = None
    _CallbackHandler.state = None

    server: HTTPServer | None = None

    async def redirect_handler(authorization_url: str) -> None:
        nonlocal server
        server = HTTPServer(("127.0.0.1", port), _CallbackHandler)
        server_thread = threading.Thread(target=server.handle_request, daemon=True)
        server_thread.start()
        logger.info("Opening browser for MCP OAuth authorization...")
        webbrowser.open(authorization_url)

    async def callback_handler() -> tuple[str, str | None]:
        loop = asyncio.get_running_loop()
        try:
            completed = await loop.run_in_executor(None, lambda: event.wait(_DEFAULT_TIMEOUT))
            if not completed:
                raise TimeoutError(f"Timed out waiting for OAuth callback on http://127.0.0.1:{port}")
            code = _CallbackHandler.code
            state = _CallbackHandler.state
            if not code:
                raise RuntimeError("OAuth callback did not receive an authorization code")
            return code, state
        finally:
            if server is not None:
                server.server_close()

    return redirect_handler, callback_handler


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------


def create_oauth_provider(server_url: str) -> httpx.Auth:
    """Create an ``httpx.Auth`` that performs browser-based OAuth for *server_url*.

    Tokens are cached to ``~/.databao/mcp-tokens/`` so the browser flow only
    triggers on first use (or when the refresh token expires).
    """
    from mcp.shared.auth import OAuthClientMetadata

    port = _find_free_port()
    redirect_uri = f"http://127.0.0.1:{port}/callback"

    client_metadata = OAuthClientMetadata(
        redirect_uris=[redirect_uri],
        token_endpoint_auth_method="none",
        grant_types=["authorization_code", "refresh_token"],
        response_types=["code"],
        client_name="databao",
        scope="openid profile",
    )

    storage: TokenStorage = FileTokenStorage(server_url)
    redirect_handler, callback_handler = _make_handlers(port)

    return OAuthClientProvider(
        server_url=server_url,
        client_metadata=client_metadata,
        storage=storage,
        redirect_handler=redirect_handler,
        callback_handler=callback_handler,
        timeout=_DEFAULT_TIMEOUT,
    )
