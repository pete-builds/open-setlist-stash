"""HTTP routers grouped by domain.

Each module exports a ``router`` (an ``APIRouter``) mounted by
``server.build_app``. Domain modules (``setlist_stash.auth``,
``setlist_stash.leagues``, etc.) hold the business logic; these routers only
parse -> validate -> dispatch -> respond.
"""
