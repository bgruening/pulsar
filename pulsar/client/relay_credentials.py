"""On-disk relay credentials file.

Holds the long-lived refresh token written by ``pulsar-config --login``. The
daemon rotates this file every time it exchanges the refresh token for a new
access JWT, so the file must be writable and securely permissioned.
"""

import json
import logging
import os
import stat
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict, Optional

log = logging.getLogger(__name__)


SAFE_MODE = 0o600


class CredentialsFile:
    """Wrapper around a JSON credentials file with mode-checking and atomic writes."""

    def __init__(self, path: str):
        self.path = os.path.abspath(path)

    def exists(self) -> bool:
        return os.path.isfile(self.path)

    def load(self) -> Optional[Dict[str, Any]]:
        """Read the credentials file. Returns ``None`` if it does not exist.

        Logs a warning if the file is more permissive than mode 0600.
        """
        if not self.exists():
            return None
        try:
            mode = stat.S_IMODE(os.stat(self.path).st_mode)
        except OSError as exc:
            log.warning("Failed to stat credentials file %s: %s", self.path, exc)
            mode = None
        if mode is not None and (mode & 0o077):
            log.warning(
                "Relay credentials file %s has mode 0%o; recommended is 0%o.",
                self.path,
                mode,
                SAFE_MODE,
            )
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError) as exc:
            log.error("Failed to read relay credentials at %s: %s", self.path, exc)
            return None

    def save(self, data: Dict[str, Any]) -> None:
        """Atomically write the credentials file with mode 0600.

        Writes to ``path.tmp``, fsyncs, sets perms, then renames over the
        original. The temp file inherits the destination's directory.
        """
        directory = os.path.dirname(self.path) or "."
        os.makedirs(directory, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(prefix=".pulsar-relay-cred-", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.chmod(tmp_path, SAFE_MODE)
            os.replace(tmp_path, self.path)
        except Exception:
            # Best-effort cleanup of the temp file on failure.
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise


def utcnow_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()
