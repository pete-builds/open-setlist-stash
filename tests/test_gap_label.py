"""Unit tests for the picker gap label helper.

``_gap_label`` turns a song's "shows since last play" gap into the muted
hint shown next to a pick. It must degrade to an empty string whenever gap
is unknown so the shared-repo Phish deployment (which may omit gap) renders a
plain song title rather than erroring.
"""

from __future__ import annotations

import pytest

from setlist_stash.web_helpers import _gap_label


@pytest.mark.parametrize(
    ("gap", "expected"),
    [
        (0, "last show"),
        # phish.net convention: the show being picked counts. Tweezer Reprise
        # last played 9/6 with only 10/2 since (gap 1) is a 2 show gap on
        # 10/3, not the "1 show gap" this used to print.
        (1, "2 show gap"),
        (2, "3 show gap"),
        (6, "7 show gap"),
        (320, "321 show gap"),
        (None, ""),
        (-1, ""),
        ("not-a-number", ""),
    ],
)
def test_gap_label(gap: object, expected: str) -> None:
    assert _gap_label(gap) == expected
