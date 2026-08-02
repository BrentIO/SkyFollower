"""
Shared open path for a runner's local SQLite staging database.

Every runner that stages parsed data locally before writing to Redis uses
the same fixed, unmounted path (`/app/data/staging.db`). Ofelia schedules
runners in `container =` mode, which starts the *same* container on every
scheduled run rather than a fresh one, so anything left on that path from a
prior run is still there. A bare `CREATE TABLE ...` against a file that
already has those tables raises `sqlite3.OperationalError: table ... already
exists`, failing the run and leaving Redis's previous data to age out with
no successful refresh ever landing again.

`open_staging_db()` is the single choke point every runner opens its
staging database through, so the delete-then-create invariant is enforced
once here rather than needing to be re-implemented (or forgotten) in each
runner.
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
