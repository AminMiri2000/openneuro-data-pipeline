"""Build a human-readable summary of what went wrong in a run."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # avoids importing the pipeline (and dbt) just to format text
    from ..pipeline import PipelineResult

# Long lists help nobody in an email; the rest is in the logs and manifests.
MAX_DETAILS_PER_ISSUE = 10


class Severity(StrEnum):
    """How urgent an issue is. FAILURE means data was not refreshed."""

    FAILURE = "failure"
    WARNING = "warning"


@dataclass(frozen=True)
class Issue:
    """One problem worth telling a person about."""

    severity: Severity
    title: str
    details: tuple[str, ...] = ()


@dataclass
class Digest:
    """Everything one alert email says about one run."""

    source: str  # e.g. "GitHub Actions daily refresh" or "Dagster daily_refresh"
    issues: list[Issue] = field(default_factory=list)
    link: str | None = None  # where to look next: run page, workflow logs, ...
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    # Warnings still present but already reported in an earlier email.
    ongoing_warnings: int = 0

    @property
    def has_failures(self) -> bool:
        return any(issue.severity is Severity.FAILURE for issue in self.issues)

    @property
    def subject(self) -> str:
        if self.has_failures:
            state = "FAILED"
        elif self.issues:
            state = f"{len(self.issues)} warning{'s' if len(self.issues) != 1 else ''}"
        else:
            state = "OK"
        return f"[openneuro-pipeline] {self.source}: {state}"

    def render_text(self) -> str:
        """Plain-text body: readable in any mail client, including on a phone."""
        lines = [
            f"{self.source} — {self.created_at:%Y-%m-%d %H:%M} UTC",
            "",
        ]
        if not self.issues:
            lines.append("No problems found.")
        for issue in sorted(self.issues, key=lambda i: i.severity is not Severity.FAILURE):
            lines.append(f"{issue.severity.value.upper()}: {issue.title}")
            shown = issue.details[:MAX_DETAILS_PER_ISSUE]
            lines.extend(f"  - {detail}" for detail in shown)
            if len(issue.details) > len(shown):
                lines.append(f"  ... and {len(issue.details) - len(shown)} more")
            lines.append("")
        if self.ongoing_warnings:
            lines.append(
                f"Also still present, reported earlier: {self.ongoing_warnings} warning(s)."
            )
            lines.append("")
        if self.link:
            lines.append(f"Details: {self.link}")
        return "\n".join(lines).rstrip() + "\n"


def digest_from_pipeline_result(
    result: PipelineResult, source: str, link: str | None = None
) -> Digest:
    """Collect the issues a CLI run reported: failed items and dbt warnings."""
    digest = Digest(source=source, link=link)
    for extract in result.extracts:
        if extract.errors:
            digest.issues.append(
                Issue(
                    Severity.WARNING,
                    f"{len(extract.errors)} item(s) failed in '{extract.step}' "
                    "(kept for retry on the next run)",
                    tuple(extract.errors),
                )
            )
    if result.transform and result.transform.warning_details:
        digest.issues.append(
            Issue(
                Severity.WARNING,
                f"{len(result.transform.warning_details)} data-quality check(s) warned",
                tuple(result.transform.warning_details),
            )
        )
    return digest


def failure_digest(source: str, error: BaseException, link: str | None = None) -> Digest:
    """Digest for a run that stopped with an error."""
    return Digest(
        source=source,
        issues=[Issue(Severity.FAILURE, "The run stopped with an error", (str(error),))],
        link=link,
    )
