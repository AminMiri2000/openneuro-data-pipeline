import smtplib

import pytest

from openneuro_pipeline.alerts import Digest, EmailSettings, Issue, Severity, send_digest
from openneuro_pipeline.alerts.digest import MAX_DETAILS_PER_ISSUE, failure_digest
from openneuro_pipeline.errors import ConfigError


def test_subject_reflects_worst_issue():
    assert Digest("X").subject.endswith("X: OK")
    warn = Digest("X", issues=[Issue(Severity.WARNING, "w")])
    assert warn.subject.endswith("X: 1 warning")
    assert failure_digest("X", RuntimeError("boom")).subject.endswith("X: FAILED")


def test_body_puts_failures_first_and_truncates_details():
    digest = Digest(
        "Daily refresh",
        issues=[
            Issue(Severity.WARNING, "many", tuple(f"d{i}" for i in range(25))),
            Issue(Severity.FAILURE, "broken", ("why",)),
        ],
        link="https://example.test/run/1",
    )
    body = digest.render_text()
    assert body.index("FAILURE: broken") < body.index("WARNING: many")
    assert body.count("  - d") == MAX_DETAILS_PER_ISSUE
    assert "... and 15 more" in body
    assert body.rstrip().endswith("Details: https://example.test/run/1")


def test_email_settings_from_env(monkeypatch):
    for name in ("ALERT_SMTP_USER", "ALERT_SMTP_PASSWORD", "ALERT_EMAIL_TO"):
        monkeypatch.delenv(name, raising=False)
    assert EmailSettings.from_env() is None  # not configured: alerts are optional

    monkeypatch.setenv("ALERT_SMTP_USER", "me@gmail.com")
    with pytest.raises(ConfigError, match="both"):
        EmailSettings.from_env()

    monkeypatch.setenv("ALERT_SMTP_PASSWORD", "app-password")
    monkeypatch.setenv("ALERT_EMAIL_TO", "a@x.test, b@x.test")
    settings = EmailSettings.from_env()
    assert settings.recipients == ("a@x.test", "b@x.test")
    assert (settings.host, settings.port) == ("smtp.gmail.com", 465)


class FakeSMTP:
    sent: list = []
    fail_login = False

    def __init__(self, host, port, context, timeout):
        self.host, self.port = host, port

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def login(self, user, password):
        if FakeSMTP.fail_login:
            raise smtplib.SMTPAuthenticationError(535, b"bad credentials")

    def send_message(self, message):
        FakeSMTP.sent.append(message)


@pytest.fixture
def fake_smtp(monkeypatch):
    FakeSMTP.sent, FakeSMTP.fail_login = [], False
    monkeypatch.setattr("openneuro_pipeline.alerts.email.smtplib.SMTP_SSL", FakeSMTP)
    return FakeSMTP


SETTINGS = EmailSettings(username="me@gmail.com", password="pw", recipients=("me@gmail.com",))


def test_send_digest_builds_message(fake_smtp):
    assert send_digest(failure_digest("Daily", RuntimeError("boom")), SETTINGS)
    [message] = fake_smtp.sent
    assert message["To"] == "me@gmail.com"
    assert "FAILED" in message["Subject"]
    assert "boom" in message.get_content()


def test_send_failure_is_logged_not_raised(fake_smtp, caplog):
    fake_smtp.fail_login = True
    assert send_digest(Digest("X"), SETTINGS) is False
    assert "could not send email" in caplog.text


def test_unconfigured_email_logs_digest(caplog):
    caplog.set_level("INFO")
    assert send_digest(Digest("X", issues=[Issue(Severity.WARNING, "w")]), None) is False
    assert "WARNING: w" in caplog.text


def test_memory_reports_each_warning_once(tmp_path):
    from openneuro_pipeline.alerts import AlertMemory
    from openneuro_pipeline.storage import LocalObjectStore

    memory = AlertMemory(LocalObjectStore(tmp_path))

    def run(*details, failure=False):
        issues = [Issue(Severity.WARNING, "checks warned", details)] if details else []
        if failure:
            issues.append(Issue(Severity.FAILURE, "broken"))
        return memory.only_new(Digest("Daily", issues=issues))

    first = run("dup_ids: Got 1 result", "ds1@1.0: HTTP 500")
    assert len(first.issues[0].details) == 2

    second = run("dup_ids: Got 2 results", "ds1@1.0: HTTP 500")  # same warnings, new counts
    assert second.issues == [] and second.ongoing_warnings == 2

    third = run("dup_ids: Got 2 results", "new_check: Got 1 result")
    assert third.issues[0].details == ("new_check: Got 1 result",)
    assert "Also still present, reported earlier: 1" in third.render_text()

    run()  # everything resolved
    assert run("dup_ids: Got 1 result").issues  # a returning warning is new again
    assert run(failure=True).has_failures  # failures are never filtered
