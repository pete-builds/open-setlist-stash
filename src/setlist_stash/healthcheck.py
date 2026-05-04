"""Container HEALTHCHECK script.

Hits ``/healthz`` over loopback. Exits 0 on 200, 1 otherwise. Uses the stdlib
``urllib`` so no third-party deps are needed inside the runtime image.
"""

from __future__ import annotations

import sys
import urllib.error
import urllib.request

from setlist_stash.config import get_settings


def main() -> int:
    cfg = get_settings()
    url = f"http://127.0.0.1:{cfg.app_port}/healthz"
    try:
        with urllib.request.urlopen(url, timeout=3) as resp:  # noqa: S310 (loopback only)
            return 0 if resp.status == 200 else 1
    except (urllib.error.URLError, TimeoutError, OSError):
        return 1


if __name__ == "__main__":
    sys.exit(main())
