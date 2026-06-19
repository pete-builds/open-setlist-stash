"""Unit tests for the picker gap label helper.

``_gap_label`` turns a song's "shows since last play" gap into the muted
hint shown next to a pick. It must degrade to an empty string whenever gap
is unknown so the shared-repo Phish deployment (which may omit gap) renders a
plain song title rather than erroring.
"""

from __future__ import annotations

import pytest

from setlist_stash.server import _gap_label


@pytest.mark.parametrize(
    ("gap", "expected"),
    [
        (0, "last show"),
        (1, "1 show gap"),
        (2, "2 show gap"),
        (6, "6 show gap"),
        (320, "320 show gap"),
        (None, ""),
        (-1, ""),
        ("not-a-number", ""),
    ],
)
def test_gap_label(gap: object, expected: str) -> None:
    assert _gap_label(gap) == expected
