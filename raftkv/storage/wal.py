import os
import sqlite3
from typing import List, Tuple, Optional, Dict, Any
from raftkv.proto import raft_pb2

class WAL:
    """
    Durable Write-Ahead Log for Raft persistent state using SQLite with WAL mode.
    Guarantees ACID crash safety for currentTerm, votedFor, log entries, and snapshot metadata.
    """
    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=FULL;")
        self._init_schema()

    def _init_schema(self):
        with self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS log (
                    log_index INTEGER PRIMARY KEY,
                    term INTEGER NOT NULL,
                    op INTEGER NOT NULL,
                    key TEXT,
                    value TEXT,
                    client_id TEXT,
                    sequence_num INTEGER
                );
            """)

    def load_state(self) -> Tuple[int, Optional[str], int, int, List[raft_pb2.LogEntry]]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT key, value FROM metadata")
        meta = dict(cursor.fetchall())

        current_term = int(meta.get("current_term", 0))
        voted_for = meta.get("voted_for", None)
        if voted_for == "":
            voted_for = None
        last_included_index = int(meta.get("last_included_index", 0))
        last_included_term = int(meta.get("last_included_term", 0))

        cursor.execute("""
            SELECT log_index, term, op, key, value, client_id, sequence_num 
            FROM log 
            ORDER BY log_index ASC
        """)
        rows = cursor.fetchall()
        entries = []
        for r in rows:
            entry = raft_pb2.LogEntry(
                index=r[0],
                term=r[1],
                op=r[2],
                key=r[3] or "",
                value=r[4] or "",
                client_id=r[5] or "",
                sequence_num=r[6] or 0
            )
            entries.append(entry)

        return current_term, voted_for, last_included_index, last_included_term, entries

    def save_metadata(self, current_term: int, voted_for: Optional[str]):
        v_str = voted_for if voted_for is not None else ""
        with self.conn:
            self.conn.execute(
                "INSERT INTO metadata (key, value) VALUES ('current_term', ?) ON CONFLICT(key) DO UPDATE SET value=?",
                (str(current_term), str(current_term))
            )
            self.conn.execute(
                "INSERT INTO metadata (key, value) VALUES ('voted_for', ?) ON CONFLICT(key) DO UPDATE SET value=?",
                (v_str, v_str)
            )

    def save_snapshot_metadata(self, last_included_index: int, last_included_term: int):
        with self.conn:
            self.conn.execute(
                "INSERT INTO metadata (key, value) VALUES ('last_included_index', ?) ON CONFLICT(key) DO UPDATE SET value=?",
                (str(last_included_index), str(last_included_index))
            )
            self.conn.execute(
                "INSERT INTO metadata (key, value) VALUES ('last_included_term', ?) ON CONFLICT(key) DO UPDATE SET value=?",
                (str(last_included_term), str(last_included_term))
            )

    def append_entries(self, entries: List[raft_pb2.LogEntry]):
        if not entries:
            return
        with self.conn:
            self.conn.executemany(
                """
                INSERT OR REPLACE INTO log (log_index, term, op, key, value, client_id, sequence_num)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [(e.index, e.term, e.op, e.key, e.value, e.client_id, e.sequence_num) for e in entries]
            )

    def truncate_log_from(self, from_index: int):
        """Discards all entries starting at from_index (inclusive)."""
        with self.conn:
            self.conn.execute("DELETE FROM log WHERE log_index >= ?", (from_index,))

    def compact_log_up_to(self, last_included_index: int):
        """Discards entries up to last_included_index (inclusive)."""
        with self.conn:
            self.conn.execute("DELETE FROM log WHERE log_index <= ?", (last_included_index,))

    def clear_all_log(self):
        with self.conn:
            self.conn.execute("DELETE FROM log")

    def close(self):
        self.conn.close()
