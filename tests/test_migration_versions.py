"""Static guards on the ``migrations/`` directory.

``run_migrations`` decides what to apply by version NUMBER alone. Two files
sharing a number therefore both run on a fresh database (the pending list is
built before either stamps) but only the first runs on any database that
already holds that version. Every DB-backed test starts fresh, so the suite
cannot see the skip. That is exactly how ``013_email_reminders.sql`` reached
main beside ``013_auth_token_email.sql``: green CI, and a deploy that would
have skipped it on both production databases.
"""

from __future__ import annotations

import re
from collections import Counter

from setlist_stash.migrate import discover_migrations

_STAMP_RE = re.compile(r"INSERT INTO schema_version \(version\) VALUES \((\d+)\)")


def test_migration_versions_are_unique() -> None:
    counts = Counter(version for version, _ in discover_migrations())
    dupes = {
        version: sorted(p.name for v, p in discover_migrations() if v == version)
        for version, n in counts.items()
        if n > 1
    }
    assert not dupes, f"migrations share a version number: {dupes}"


def test_migration_self_stamp_matches_filename() -> None:
    """A file that stamps a different version than its name lies to the runner."""
    mismatched = {}
    for version, path in discover_migrations():
        stamps = {int(s) for s in _STAMP_RE.findall(path.read_text(encoding="utf-8"))}
        if stamps and stamps != {version}:
            mismatched[path.name] = sorted(stamps)
    assert not mismatched, f"self-stamp disagrees with filename: {mismatched}"
