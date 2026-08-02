import sqlite3

from shared.sqlite_staging import open_staging_db

_SCHEMA = "CREATE TABLE aircraft (icao_hex TEXT PRIMARY KEY);"


class TestOpenStagingDb:
    def test_creates_parent_directory(self, tmp_path):
        db_path = str(tmp_path / "nested" / "staging.db")
        conn = open_staging_db(db_path, _SCHEMA)
        conn.close()
        assert (tmp_path / "nested").is_dir()

    def test_applies_schema(self, tmp_path):
        db_path = str(tmp_path / "staging.db")
        conn = open_staging_db(db_path, _SCHEMA)
        conn.execute("INSERT INTO aircraft (icao_hex) VALUES ('A8AE7F')")
        conn.commit()
        rows = conn.execute("SELECT icao_hex FROM aircraft").fetchall()
        conn.close()
        assert [row["icao_hex"] for row in rows] == ["A8AE7F"]

    def test_uses_row_factory(self, tmp_path):
        db_path = str(tmp_path / "staging.db")
        conn = open_staging_db(db_path, _SCHEMA)
        assert conn.row_factory is sqlite3.Row
        conn.close()

    def test_deletes_pre_existing_file_before_reopening(self, tmp_path):
        db_path = str(tmp_path / "staging.db")

        conn = open_staging_db(db_path, _SCHEMA)
        conn.execute("INSERT INTO aircraft (icao_hex) VALUES ('A8AE7F')")
        conn.commit()
        conn.close()

        # A bare CREATE TABLE against the same file would raise
        # "table aircraft already exists" if the stale file survived.
        conn = open_staging_db(db_path, _SCHEMA)
        rows = conn.execute("SELECT icao_hex FROM aircraft").fetchall()
        conn.close()
        assert rows == []
