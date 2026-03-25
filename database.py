"""SQLite helpers for storing processing run history."""

import json
import os
import sqlite3
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "meditech.db")


def _connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = _connect()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS runs (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date    TEXT NOT NULL,
            file_type   TEXT NOT NULL,
            filename    TEXT NOT NULL,
            raw_rows    INTEGER,
            clean_rows  INTEGER,
            summary_rows INTEGER,
            stats_json  TEXT
        );
        CREATE TABLE IF NOT EXISTS run_data (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id    INTEGER NOT NULL,
            data_type TEXT NOT NULL,
            csv_content TEXT NOT NULL,
            FOREIGN KEY (run_id) REFERENCES runs(id)
        );
    """)
    conn.commit()
    conn.close()


def save_run(file_type, filename, stats, clean_csv, summary_csv):
    """Save a processing run and its CSV outputs. Returns the run id."""
    conn = _connect()
    cur = conn.execute(
        """INSERT INTO runs (run_date, file_type, filename, raw_rows, clean_rows, summary_rows, stats_json)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            file_type,
            filename,
            stats.get("raw_rows", 0),
            stats.get("clean_rows", 0),
            stats.get("summary_rows", 0),
            json.dumps(stats),
        ),
    )
    run_id = cur.lastrowid
    conn.execute(
        "INSERT INTO run_data (run_id, data_type, csv_content) VALUES (?, ?, ?)",
        (run_id, "clean", clean_csv),
    )
    conn.execute(
        "INSERT INTO run_data (run_id, data_type, csv_content) VALUES (?, ?, ?)",
        (run_id, "summary", summary_csv),
    )
    conn.commit()
    conn.close()
    return run_id


def get_runs():
    """Return all runs, newest first."""
    conn = _connect()
    rows = conn.execute("SELECT * FROM runs ORDER BY id DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_run(run_id):
    conn = _connect()
    row = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_run_data(run_id, data_type):
    """Return the CSV content string for a run."""
    conn = _connect()
    row = conn.execute(
        "SELECT csv_content FROM run_data WHERE run_id = ? AND data_type = ?",
        (run_id, data_type),
    ).fetchone()
    conn.close()
    return row["csv_content"] if row else None


def delete_run(run_id):
    conn = _connect()
    conn.execute("DELETE FROM run_data WHERE run_id = ?", (run_id,))
    conn.execute("DELETE FROM runs WHERE id = ?", (run_id,))
    conn.commit()
    conn.close()
