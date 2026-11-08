"""Logging set-up shared by every entry point (CLI today, orchestrator later)."""

import logging

LOG_FORMAT = "%(asctime)s %(levelname)-7s %(name)s: %(message)s"


def configure_logging(verbose: bool = False) -> None:
    """Configure root logging once; safe to call repeatedly."""
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO, format=LOG_FORMAT)
    # Retry/connection chatter from urllib3 is noise at INFO level.
    logging.getLogger("urllib3").setLevel(logging.WARNING)
