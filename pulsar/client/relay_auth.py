"""
JWT authentication manager for pulsar-relay.

The manager holds a short-lived access-token cache. The actual *acquire a
fresh token* policy is pluggable: PasswordAuthenticator (legacy), or
RefreshTokenAuthenticator (preferred, written by ``pulsar-config --login``).

A daemon's typical lifecycle is:

1. ``pulsar-config --login`` performs a one-time browser-based device-flow
   sign-in and writes ``relay_credentials.json`` containing a refresh token.
2. The daemon constructs a ``RelayAuthManager`` with a
   ``RefreshTokenAuthenticator`` pointing at that file.
3. The manager exchanges the refresh token for an access JWT on demand,
   atomically rewrites the credentials file with the rotated refresh token,
   and caches the access JWT until it nears expiry.
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Optional, cast

import requests

from .relay_credentials import CredentialsFile, utcnow_iso

log = logging.getLogger(__name__)


class RelayAuthError(Exception):
    """Raised when the relay rejects an authentication attempt."""


class _Authenticator:
    """Strategy interface: produce a fresh ``(access_token, expires_in_seconds)``."""

    def authenticate(self) -> tuple[str, int]:
        raise NotImplementedError


class PasswordAuthenticator(_Authenticator):
    """Username/password against ``/auth/login`` (legacy path).

    If the relay returns a refresh token (new behavior), it is captured and
    written to the optional ``credentials_file`` so subsequent runs can use
    the refresh-token path.
    """

    def __init__(
        self,
        relay_url: str,
        username: str,
        password: str,
        *,
        credentials_file: Optional[CredentialsFile] = None,
        timeout: int = 10,
    ):
        self.relay_url = relay_url.rstrip("/")
        self.username = username
        self.password = password
        self._credentials_file = credentials_file
        self._timeout = timeout

    def authenticate(self) -> tuple[str, int]:
        url = f"{self.relay_url}/auth/login"
        try:
            resp = requests.post(
                url,
                data={
                    "username": self.username,
                    "password": self.password,
                    "grant_type": "password",
                },
                timeout=self._timeout,
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise RelayAuthError(f"pulsar-relay password authentication failed: {exc}") from exc

        body = resp.json()
        access_token = body["access_token"]
        expires_in = int(body.get("expires_in", 3600))

        refresh_token = body.get("refresh_token")
        if refresh_token and self._credentials_file is not None:
            self._credentials_file.save(
                {
                    "relay_url": self.relay_url,
                    "refresh_token": refresh_token,
                    "issued_at": utcnow_iso(),
                }
            )
            log.info("Captured refresh token from /auth/login into %s", self._credentials_file.path)

        return access_token, expires_in


class RefreshTokenAuthenticator(_Authenticator):
    """Rotate a refresh token at ``/auth/token/refresh``.

    On success we atomically rewrite ``credentials_file`` so the next process
    inherits the rotated token. On 401 (revoked or expired) we leave the file
    in place and raise — the operator must re-run ``pulsar-config --login``.
    """

    def __init__(
        self,
        relay_url: str,
        credentials_file: CredentialsFile,
        *,
        timeout: int = 10,
    ):
        self.relay_url = relay_url.rstrip("/")
        self._credentials_file = credentials_file
        self._timeout = timeout

    def authenticate(self) -> tuple[str, int]:
        creds = self._credentials_file.load()
        if creds is None or not creds.get("refresh_token"):
            raise RelayAuthError(
                f"No refresh token at {self._credentials_file.path}; "
                "run `pulsar-config --login` to bootstrap one."
            )

        url = f"{self.relay_url}/auth/token/refresh"
        try:
            resp = requests.post(
                url,
                json={"refresh_token": creds["refresh_token"]},
                timeout=self._timeout,
            )
        except requests.RequestException as exc:
            raise RelayAuthError(f"pulsar-relay refresh failed (network): {exc}") from exc

        if resp.status_code == 401:
            raise RelayAuthError(
                "Refresh token rejected (revoked or expired). "
                "Re-run `pulsar-config --login`."
            )
        try:
            resp.raise_for_status()
        except requests.HTTPError as exc:
            raise RelayAuthError(f"pulsar-relay refresh failed: HTTP {resp.status_code}") from exc

        body = resp.json()
        access_token = body["access_token"]
        expires_in = int(body.get("expires_in", 3600))
        new_refresh = body.get("refresh_token")
        if new_refresh:
            # Persist the rotated refresh token. If this write fails we still
            # have a usable access token in hand for *this* process — but the
            # next process will choke. Surface the error.
            self._credentials_file.save(
                {
                    "relay_url": self.relay_url,
                    "refresh_token": new_refresh,
                    "issued_at": utcnow_iso(),
                }
            )
        return access_token, expires_in


class RelayAuthManager:
    """Thread-safe access-JWT cache backed by a pluggable authenticator.

    The legacy constructor signature (``relay_url, username, password``) is
    preserved so existing callers keep working.
    """

    def __init__(
        self,
        relay_url: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        *,
        authenticator: Optional[_Authenticator] = None,
        credentials_file: Optional[str] = None,
    ):
        self.relay_url = relay_url.rstrip("/")
        self._token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None
        self._lock = threading.Lock()
        # Refresh access JWT 5 minutes before expiry.
        self._refresh_buffer_seconds = 300

        cred_file = CredentialsFile(credentials_file) if credentials_file else None

        if authenticator is not None:
            self._authenticator = authenticator
        elif cred_file is not None and cred_file.exists():
            # Prefer the refresh-token path when a credentials file is present.
            self._authenticator = RefreshTokenAuthenticator(self.relay_url, cred_file)
        elif username is not None and password is not None:
            self._authenticator = PasswordAuthenticator(
                self.relay_url, username, password, credentials_file=cred_file
            )
        else:
            raise ValueError(
                "RelayAuthManager needs either an Authenticator, a credentials file, "
                "or username+password."
            )

    # ---- public API ---------------------------------------------------------

    def get_token(self) -> str:
        with self._lock:
            if self._is_token_valid():
                return cast(str, self._token)
            log.debug("Fetching a fresh access token from pulsar-relay at %s", self.relay_url)
            access_token, expires_in = self._authenticator.authenticate()
            self._token = access_token
            self._token_expiry = datetime.now(tz=timezone.utc) + timedelta(seconds=expires_in)
            log.info("Acquired pulsar-relay access token (expires in %ds)", expires_in)
            return access_token

    def invalidate(self) -> None:
        with self._lock:
            self._token = None
            self._token_expiry = None
            log.debug("Invalidated pulsar-relay access token cache")

    # ---- internals ----------------------------------------------------------

    def _is_token_valid(self) -> bool:
        if self._token is None or self._token_expiry is None:
            return False
        seconds_left = (self._token_expiry - datetime.now(tz=timezone.utc)).total_seconds()
        return seconds_left > self._refresh_buffer_seconds


__all__ = [
    "RelayAuthManager",
    "RelayAuthError",
    "PasswordAuthenticator",
    "RefreshTokenAuthenticator",
]
