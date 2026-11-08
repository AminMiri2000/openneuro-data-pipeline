"""Remember which warnings were already reported, so each is emailed once.

A persistent upstream issue (say, a dataset that lists a participant twice)
would otherwise produce the same email every day until people stop reading
them, which is how real alerts get missed. A warning is emailed when it first
appears, and again only if it disappears and later comes back. Failures are
never filtered.

Warnings are identified by the text before the first ": " in each detail line
(a dbt test name, a "dataset@snapshot", a check name), so a changing count in
the message does not make an old warning look new.
"""

from __future__ import annotations

from dataclasses import replace

from ..storage import ObjectStore, layout
from ..storage.formats import read_json, write_json
from .digest import Digest, Severity

STATE_NAME = "alert_memory"


def warning_key(detail: str) -> str:
    return detail.split(": ", 1)[0]


class AlertMemory:
    """Set of warning keys reported in the previous run, kept in the raw layer.

    Stored next to the other pipeline state (`_system/state/`), so the daily
    GitHub Actions run carries it between runs with the raw-data cache.
    """

    def __init__(self, store: ObjectStore) -> None:
        self._store = store
        self._key = layout.state_key(STATE_NAME)

    def _load(self) -> set[str]:
        if not self._store.exists(self._key):
            return set()
        return set(read_json(self._store, self._key).get("reported_warnings", []))

    def only_new(self, digest: Digest) -> Digest:
        """Return the digest without warnings already reported last time, and remember this run's.

        The stored set is replaced by the current warnings: a warning that
        clears and later returns is reported again.
        """
        previous = self._load()
        current: set[str] = set()
        issues = []
        ongoing = 0
        for issue in digest.issues:
            if issue.severity is not Severity.WARNING:
                issues.append(issue)
                continue
            keys = [warning_key(detail) for detail in issue.details]
            current.update(keys)
            new_details = tuple(
                d for d, k in zip(issue.details, keys, strict=True) if k not in previous
            )
            ongoing += len(issue.details) - len(new_details)
            if new_details:
                issues.append(replace(issue, details=new_details))
        write_json(self._store, self._key, {"reported_warnings": sorted(current)})
        return replace(digest, issues=issues, ongoing_warnings=ongoing)
