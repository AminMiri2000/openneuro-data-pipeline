"""Send digests by email over SMTP (Gmail by default).

Configured entirely by environment variables, so no credentials ever live in
the repository. For Gmail, ALERT_SMTP_PASSWORD must be an *app password*
(Google Account → Security → 2-Step Verification → App passwords), not the
account password.
"""

from __future__ import annotations

import logging
import os
import smtplib
import ssl
from dataclasses import dataclass
from email.message import EmailMessage

from ..errors import ConfigError
from .digest import Digest

log = logging.getLogger(__name__)

GMAIL_HOST = "smtp.gmail.com"
GMAIL_SSL_PORT = 465


@dataclass(frozen=True)
class EmailSettings:
    """SMTP connection and addressing.

    Attributes:
        username: SMTP login, for Gmail the full address that sends the mail.
        password: SMTP password; for Gmail an app password.
        recipients: Who receives alerts (defaults to the sender).
    """

    username: str
    password: str
    recipients: tuple[str, ...]
    host: str = GMAIL_HOST
    port: int = GMAIL_SSL_PORT

    @classmethod
    def from_env(cls) -> EmailSettings | None:
        """Read settings from ALERT_* variables; None when alerts are not set up."""
        username = os.environ.get("ALERT_SMTP_USER", "").strip()
        password = os.environ.get("ALERT_SMTP_PASSWORD", "").strip()
        if not username and not password:
            return None
        if not username or not password:
            raise ConfigError("Set both ALERT_SMTP_USER and ALERT_SMTP_PASSWORD, or neither.")
        recipients = tuple(
            address.strip()
            for address in os.environ.get("ALERT_EMAIL_TO", username).split(",")
            if address.strip()
        )
        try:
            port = int(os.environ.get("ALERT_SMTP_PORT", str(GMAIL_SSL_PORT)))
        except ValueError as exc:
            raise ConfigError(f"ALERT_SMTP_PORT must be a number: {exc}") from exc
        return cls(
            username=username,
            password=password,
            recipients=recipients,
            host=os.environ.get("ALERT_SMTP_HOST", GMAIL_HOST),
            port=port,
        )


def build_message(digest: Digest, settings: EmailSettings) -> EmailMessage:
    message = EmailMessage()
    message["Subject"] = digest.subject
    message["From"] = settings.username
    message["To"] = ", ".join(settings.recipients)
    message.set_content(digest.render_text())
    return message


def send_digest(digest: Digest, settings: EmailSettings | None) -> bool:
    """Email the digest. Returns False (and only logs) when email is not configured.

    Sending problems are logged rather than raised: a broken alert channel must
    never turn a successful data refresh into a failed one.
    """
    if settings is None:
        log.info("alerts: email not configured; digest follows\n%s", digest.render_text())
        return False
    message = build_message(digest, settings)
    try:
        with smtplib.SMTP_SSL(
            settings.host, settings.port, context=ssl.create_default_context(), timeout=30
        ) as smtp:
            smtp.login(settings.username, settings.password)
            smtp.send_message(message)
    except (smtplib.SMTPException, OSError) as exc:
        log.error(
            "alerts: could not send email (%s); digest follows\n%s", exc, digest.render_text()
        )
        return False
    log.info("alerts: sent '%s' to %s", digest.subject, ", ".join(settings.recipients))
    return True
