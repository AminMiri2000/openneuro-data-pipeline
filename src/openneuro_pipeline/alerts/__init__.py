"""Alerts: turn pipeline outcomes into a short digest and email it.

Shared by the CLI (`run --notify`, used by the daily GitHub Actions job) and
the Dagster sensors, so both runners report problems the same way.
"""

from .digest import Digest, Issue, Severity, digest_from_pipeline_result
from .email import EmailSettings, send_digest

__all__ = [
    "Digest",
    "EmailSettings",
    "Issue",
    "Severity",
    "digest_from_pipeline_result",
    "send_digest",
]
