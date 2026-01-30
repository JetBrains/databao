"""Thread state storage abstraction for MCP server persistence."""

import json
import sqlite3
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path


@dataclass
class ThreadState:
    """Serializable state to recreate thread for plot generation.

    This stores only the minimal data needed to regenerate visualizations,
    avoiding non-serializable objects like Thread, Agent, or LLM clients.
    """

    thread_id: str
    df_json: str | None  # DataFrame serialized as JSON
    text: str
    code: str | None
    original_query: str

    # Data source info to recreate agent
    data: list[dict] | None
    database_url: str | None
    context: str | None

    # Cached visualization spec (generated on first Chart tab click)
    spec_json: str | None = None  # Vega-Lite spec with data
    spec_df_json: str | None = None  # Transformed DataFrame used for the spec


class ThreadStorage(ABC):
    """Abstract interface for thread state persistence."""

    @abstractmethod
    def store(self, state: ThreadState) -> None:
        """Store thread state.

        Args:
            state: ThreadState to persist
        """
        pass

    @abstractmethod
    def get(self, thread_id: str) -> ThreadState | None:
        """Retrieve thread state by ID.

        Args:
            thread_id: Unique thread identifier

        Returns:
            ThreadState if found, None otherwise
        """
        pass

    @abstractmethod
    def delete(self, thread_id: str) -> bool:
        """Delete thread state by ID.

        Args:
            thread_id: Unique thread identifier

        Returns:
            True if deleted, False if not found
        """
        pass

    @abstractmethod
    def list_all(self) -> list[str]:
        """List all stored thread IDs.

        Returns:
            List of thread IDs
        """
        pass


class SQLiteThreadStorage(ThreadStorage):
    """SQLite-based thread storage implementation."""

    SCHEMA_VERSION = 1

    def __init__(self, db_path: Path | str):
        """Initialize SQLite storage.

        Args:
            db_path: Path to SQLite database file
        """
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_database()

    def _get_schema_version(self, conn: sqlite3.Connection) -> int:
        """Get current schema version from database."""
        try:
            cursor = conn.execute("SELECT version FROM schema_version LIMIT 1")
            row = cursor.fetchone()
            return row[0] if row else 0
        except sqlite3.OperationalError:
            return 0

    def _set_schema_version(self, conn: sqlite3.Connection, version: int) -> None:
        """Set schema version in database."""
        conn.execute("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER)")
        conn.execute("INSERT OR REPLACE INTO schema_version (rowid, version) VALUES (1, ?)", (version,))

    def _init_database(self) -> None:
        """Initialize database schema and run migrations if needed."""
        with sqlite3.connect(self._db_path) as conn:
            conn.row_factory = sqlite3.Row
            current_version = self._get_schema_version(conn)

            if current_version == 0:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS thread_states (
                        thread_id TEXT PRIMARY KEY,
                        df_json TEXT,
                        text TEXT NOT NULL,
                        code TEXT,
                        original_query TEXT NOT NULL,
                        data TEXT,
                        database_url TEXT,
                        context TEXT,
                        spec_json TEXT,
                        spec_df_json TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_created_at
                    ON thread_states(created_at)
                """)
                self._set_schema_version(conn, self.SCHEMA_VERSION)

            conn.commit()

    def _row_to_state(self, row: sqlite3.Row) -> ThreadState:
        """Convert database row to ThreadState."""
        return ThreadState(
            thread_id=row["thread_id"],
            df_json=row["df_json"],
            text=row["text"],
            code=row["code"],
            original_query=row["original_query"],
            data=json.loads(row["data"]) if row["data"] else None,
            database_url=row["database_url"],
            context=row["context"],
            spec_json=row["spec_json"],
            spec_df_json=row["spec_df_json"],
        )

    def store(self, state: ThreadState) -> None:
        """Store thread state to database."""
        try:
            with sqlite3.connect(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                conn.execute(
                    """
                    INSERT OR REPLACE INTO thread_states
                    (thread_id, df_json, text, code, original_query, data, database_url, context, spec_json, spec_df_json, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                    (
                        state.thread_id,
                        state.df_json,
                        state.text,
                        state.code,
                        state.original_query,
                        json.dumps(state.data) if state.data else None,
                        state.database_url,
                        state.context,
                        state.spec_json,
                        state.spec_df_json,
                    ),
                )
                conn.commit()
        except Exception as e:
            print(f"Warning: Failed to persist thread state to database: {e}", file=sys.stderr)

    def get(self, thread_id: str) -> ThreadState | None:
        """Retrieve thread state from database."""
        try:
            with sqlite3.connect(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    """
                    SELECT thread_id, df_json, text, code, original_query, data, database_url, context, spec_json, spec_df_json
                    FROM thread_states
                    WHERE thread_id = ?
                """,
                    (thread_id,),
                )
                row = cursor.fetchone()

                if row:
                    return self._row_to_state(row)
        except Exception as e:
            print(f"Warning: Failed to load thread state from database: {e}", file=sys.stderr)

        return None

    def delete(self, thread_id: str) -> bool:
        """Delete thread state from database."""
        try:
            with sqlite3.connect(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    """
                    DELETE FROM thread_states WHERE thread_id = ?
                """,
                    (thread_id,),
                )
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            print(f"Warning: Failed to delete thread state from database: {e}", file=sys.stderr)
            return False

    def list_all(self) -> list[str]:
        """List all stored thread IDs from database."""
        try:
            with sqlite3.connect(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("""
                    SELECT thread_id FROM thread_states ORDER BY created_at DESC
                """)
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print(f"Warning: Failed to list thread states: {e}", file=sys.stderr)
            return []
