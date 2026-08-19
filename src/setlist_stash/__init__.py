"""setlist-stash: a self-hostable setlist-prediction game driven by MCP.

Band-agnostic: the engine stores no setlist data. Point it at any MCP server
that satisfies the setlist contract (``MCP_PHISH_URL``) and brand the
deployment via the SITE_NAME env var. See docs/PHASE-4-PLAN.md for design.
"""

__version__ = "0.3.0"
