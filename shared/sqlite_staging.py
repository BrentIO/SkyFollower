"""
Shared open path for a runner's local SQLite staging database.

Ofelia schedules runners in `container =` mode, starting the same
container on every scheduled run, so a prior run's staging file at
`/app/data/staging.db` is still there. A bare `CREATE TABLE ...` against
it would raise `table ... already exists` and fail the run.

`open_staging_db()` is the single choke point every runner opens its
staging database through, so the delete-then-create invariant is
enforced once here instead of in each runner.
"""

from __future__ import annotations

import os
import sqlite3


def open_staging_db(db_path: str, schema: str) -> sqlite3.Connection:
    """Delete any existing file at `db_path`, then open a fresh SQLite
    connection with `row_factory = sqlite3.Row` and `schema` applied."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    if os.path.exists(db_path):
        os.remove(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(schema)
    return conn
