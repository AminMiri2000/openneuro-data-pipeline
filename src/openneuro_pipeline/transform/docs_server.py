"""Serve the generated dbt documentation site locally.

`dbt docs serve` is not used because it serves whatever was generated last
(or nothing), and reports Ctrl+C, the normal way to stop it, as a crash with
a traceback. This server is a few lines of standard library: it is only ever
started after a fresh `docs generate`, listens on localhost only, and stops
quietly.
"""

from __future__ import annotations

import logging
import sys
import webbrowser
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from ..errors import TransformError

log = logging.getLogger(__name__)

# Files the dbt docs page loads; without them it renders empty.
REQUIRED_FILES = ("index.html", "manifest.json", "catalog.json")


class _QuietHandler(SimpleHTTPRequestHandler):
    """Static file handler without a log line per request."""

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 (stdlib signature)
        return


class _LocalServer(ThreadingHTTPServer):
    # On Windows, SO_REUSEADDR lets a second process bind a port that is
    # already in use, so a clash would go unnoticed. Elsewhere it only allows
    # quick restarts, which is harmless.
    allow_reuse_address = sys.platform != "win32"
    daemon_threads = True  # do not wait for open browser connections on exit


def make_server(site_dir: Path, port: int) -> _LocalServer:
    """Create (but do not start) a localhost server for a generated docs site.

    Raises:
        TransformError: The site is incomplete, or the port is already in use.
    """
    missing = [name for name in REQUIRED_FILES if not (site_dir / name).is_file()]
    if missing:
        raise TransformError(f"Docs site in {site_dir} is incomplete (missing {missing})")
    handler = partial(_QuietHandler, directory=str(site_dir))
    try:
        # 127.0.0.1, not 0.0.0.0: the docs describe a local warehouse and are
        # not meant to be reachable from other machines on the network.
        return _LocalServer(("127.0.0.1", port), handler)
    except OSError as exc:
        raise TransformError(f"Cannot serve docs on port {port}: {exc}") from exc


def serve(site_dir: Path, port: int, open_browser: bool = True) -> None:
    """Serve the docs until Ctrl+C, then shut down cleanly."""
    server = make_server(site_dir, port)
    url = f"http://localhost:{server.server_address[1]}"
    log.info("docs: serving %s at %s (Ctrl+C to stop)", site_dir, url)
    if open_browser:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("docs: stopped")
    finally:
        server.server_close()
