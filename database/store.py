"""Small SQLite persistence layer for validated gaming events."""

from __future__ import annotations

import json
import math
import sqlite3
from contextlib import contextmanager
from enum import StrEnum
from pathlib import Path
from typing import Iterator

from event_generator.generator import EVENT_WEIGHTS


class StoreStatus(StrEnum):
    STORED = "stored"
    DUPLICATE = "duplicate"


class Database:
    """Persist validated events using short-lived SQLite connections."""

    def __init__(self, database_path: str | Path = "data/gaming_analytics.sqlite3") -> None:
        self.path = Path(database_path)
        if self.path.name == ":memory:":
            raise ValueError("Use a file-backed SQLite database for persistent connections")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        try:
            yield connection
        finally:
            connection.close()

    def initialize(self) -> None:
        """Create the schema if it does not exist and enable lightweight WAL mode."""
        with self._connection() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = NORMAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS players (
                    player_id TEXT PRIMARY KEY,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    is_online INTEGER NOT NULL DEFAULT 0
                        CHECK (is_online IN (0, 1))
                );

                CREATE TABLE IF NOT EXISTS processed_events (
                    event_id TEXT PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    player_id TEXT NOT NULL REFERENCES players(player_id),
                    timestamp TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_processed_events_timestamp
                    ON processed_events(timestamp);
                CREATE INDEX IF NOT EXISTS idx_processed_events_player
                    ON processed_events(player_id, timestamp);

                CREATE TABLE IF NOT EXISTS gaming_sessions (
                    session_id TEXT PRIMARY KEY,
                    player_id TEXT NOT NULL REFERENCES players(player_id),
                    game_id TEXT NOT NULL,
                    started_at TEXT,
                    ended_at TEXT,
                    duration_seconds REAL
                        CHECK (duration_seconds IS NULL OR duration_seconds >= 0),
                    status TEXT NOT NULL CHECK (status IN ('active', 'ended'))
                );

                CREATE INDEX IF NOT EXISTS idx_sessions_player
                    ON gaming_sessions(player_id, started_at);

                CREATE TABLE IF NOT EXISTS player_scores (
                    event_id TEXT PRIMARY KEY
                        REFERENCES processed_events(event_id) ON DELETE CASCADE,
                    player_id TEXT NOT NULL REFERENCES players(player_id),
                    session_id TEXT NOT NULL,
                    game_id TEXT NOT NULL,
                    score INTEGER NOT NULL CHECK (score >= 0),
                    timestamp TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS transactions (
                    event_id TEXT PRIMARY KEY
                        REFERENCES processed_events(event_id) ON DELETE CASCADE,
                    transaction_id TEXT NOT NULL,
                    player_id TEXT NOT NULL REFERENCES players(player_id),
                    amount_cents INTEGER NOT NULL CHECK (amount_cents > 0),
                    currency TEXT NOT NULL,
                    timestamp TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS event_quality (
                    quality_id INTEGER PRIMARY KEY,
                    event_id TEXT,
                    status TEXT NOT NULL
                        CHECK (status IN ('valid', 'invalid', 'duplicate')),
                    reason TEXT,
                    raw_payload TEXT NOT NULL,
                    recorded_at TEXT NOT NULL DEFAULT (
                        strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')
                    )
                );

                CREATE INDEX IF NOT EXISTS idx_event_quality_status
                    ON event_quality(status, recorded_at);
                CREATE INDEX IF NOT EXISTS idx_event_quality_event
                    ON event_quality(event_id);
                """
            )

    def store_event(self, event: dict[str, object]) -> StoreStatus:
        """Atomically persist one normalized event, ignoring repeated event IDs."""
        event_id = self._required_string(event, "event_id")
        event_type = self._required_string(event, "event_type")
        if event_type not in EVENT_WEIGHTS:
            raise ValueError(f"unsupported event_type: {event_type}")
        player_id = self._required_string(event, "player_id")
        timestamp = self._required_string(event, "timestamp")
        payload_json = json.dumps(
            event, ensure_ascii=False, separators=(",", ":"), allow_nan=False
        )

        with self._connection() as connection:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                exists = connection.execute(
                    "SELECT 1 FROM processed_events WHERE event_id = ?",
                    (event_id,),
                ).fetchone()
                if exists:
                    self._record_quality(
                        connection,
                        event_id=event_id,
                        status="duplicate",
                        reason="event_id has already been stored",
                        raw_payload=payload_json,
                    )
                    return StoreStatus.DUPLICATE

                self._upsert_player(connection, player_id, timestamp, event_type)
                connection.execute(
                    """
                    INSERT INTO processed_events
                        (event_id, event_type, player_id, timestamp, payload_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (event_id, event_type, player_id, timestamp, payload_json),
                )
                self._store_event_details(connection, event)
                self._record_quality(
                    connection,
                    event_id=event_id,
                    status="valid",
                    reason=None,
                    raw_payload=payload_json,
                )

        return StoreStatus.STORED

    def record_invalid_event(
        self,
        raw_payload: str,
        reason: str,
        event_id: str | None = None,
    ) -> None:
        """Persist the original payload and reason for a rejected event."""
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason cannot be empty")
        with self._connection() as connection:
            with connection:
                self._record_quality(
                    connection,
                    event_id=event_id,
                    status="invalid",
                    reason=reason,
                    raw_payload=raw_payload,
                )

    def record_duplicate_event(
        self,
        event: dict[str, object],
        reason: str = "event_id has already been processed in this consumer run",
    ) -> None:
        """Persist a duplicate detected before it reaches the storage insert."""
        event_id = self._required_string(event, "event_id")
        raw_payload = json.dumps(
            event, ensure_ascii=False, separators=(",", ":"), allow_nan=False
        )
        with self._connection() as connection:
            with connection:
                self._record_quality(
                    connection,
                    event_id=event_id,
                    status="duplicate",
                    reason=reason,
                    raw_payload=raw_payload,
                )

    def quality_counts(self) -> dict[str, int]:
        """Return persisted quality-record totals, including zero-count statuses."""
        counts = {"valid": 0, "invalid": 0, "duplicate": 0}
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT status, COUNT(*) AS count FROM event_quality GROUP BY status"
            ).fetchall()
        for row in rows:
            counts[row["status"]] = row["count"]
        return counts

    def table_counts(self) -> dict[str, int]:
        """Return row counts for the project tables, useful for checks and reports."""
        tables = (
            "players",
            "gaming_sessions",
            "player_scores",
            "transactions",
            "processed_events",
            "event_quality",
        )
        with self._connection() as connection:
            return {
                table: connection.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0]
                for table in tables
            }

    @staticmethod
    def _required_string(event: dict[str, object], field: str) -> str:
        value = event.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{field} must be a non-empty string")
        return value

    @staticmethod
    def _record_quality(
        connection: sqlite3.Connection,
        event_id: str | None,
        status: str,
        reason: str | None,
        raw_payload: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO event_quality (event_id, status, reason, raw_payload)
            VALUES (?, ?, ?, ?)
            """,
            (event_id, status, reason, raw_payload),
        )

    @staticmethod
    def _upsert_player(
        connection: sqlite3.Connection,
        player_id: str,
        timestamp: str,
        event_type: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO players (player_id, first_seen_at, last_seen_at, is_online)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(player_id) DO UPDATE SET
                first_seen_at = MIN(players.first_seen_at, excluded.first_seen_at),
                last_seen_at = MAX(players.last_seen_at, excluded.last_seen_at),
                is_online = CASE
                    WHEN ? = 'player_login' THEN 1
                    WHEN ? = 'player_logout' THEN 0
                    ELSE players.is_online
                END
            """,
            (
                player_id,
                timestamp,
                timestamp,
                int(event_type == "player_login"),
                event_type,
                event_type,
            ),
        )

    @classmethod
    def _store_event_details(
        cls, connection: sqlite3.Connection, event: dict[str, object]
    ) -> None:
        event_type = cls._required_string(event, "event_type")
        player_id = cls._required_string(event, "player_id")
        timestamp = cls._required_string(event, "timestamp")

        if event_type == "game_start":
            connection.execute(
                """
                INSERT INTO gaming_sessions
                    (session_id, player_id, game_id, started_at, status)
                VALUES (?, ?, ?, ?, 'active')
                """,
                (
                    cls._required_string(event, "session_id"),
                    player_id,
                    cls._required_string(event, "game_id"),
                    timestamp,
                ),
            )
        elif event_type in {"game_end", "player_logout"} and "session_id" in event:
            cls._end_session(connection, event, player_id, timestamp)
        elif event_type == "player_score":
            score = event.get("score")
            if isinstance(score, bool) or not isinstance(score, int) or score < 0:
                raise ValueError("score must be a non-negative integer")
            connection.execute(
                """
                INSERT INTO player_scores
                    (event_id, player_id, session_id, game_id, score, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    cls._required_string(event, "event_id"),
                    player_id,
                    cls._required_string(event, "session_id"),
                    cls._required_string(event, "game_id"),
                    score,
                    timestamp,
                ),
            )
        elif event_type == "transaction":
            amount = event.get("amount_cents")
            if isinstance(amount, bool) or not isinstance(amount, int) or amount <= 0:
                raise ValueError("amount_cents must be a positive integer")
            connection.execute(
                """
                INSERT INTO transactions
                    (event_id, transaction_id, player_id, amount_cents, currency, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    cls._required_string(event, "event_id"),
                    cls._required_string(event, "transaction_id"),
                    player_id,
                    amount,
                    cls._required_string(event, "currency"),
                    timestamp,
                ),
            )

    @classmethod
    def _end_session(
        cls,
        connection: sqlite3.Connection,
        event: dict[str, object],
        player_id: str,
        timestamp: str,
    ) -> None:
        session_id = cls._required_string(event, "session_id")
        game_id = cls._required_string(event, "game_id")
        duration = event.get("session_duration_seconds")
        if (
            isinstance(duration, bool)
            or not isinstance(duration, (int, float))
        ):
            raise ValueError("session_duration_seconds must be non-negative")
        try:
            duration_seconds = float(duration)
        except OverflowError as error:
            raise ValueError("session_duration_seconds is outside the supported range") from error
        if not math.isfinite(duration_seconds) or duration_seconds < 0:
            raise ValueError("session_duration_seconds must be non-negative")

        existing = connection.execute(
            "SELECT player_id, game_id FROM gaming_sessions WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        if existing and (
            existing["player_id"] != player_id or existing["game_id"] != game_id
        ):
            raise ValueError("session end does not match the stored player and game")

        connection.execute(
            """
            INSERT INTO gaming_sessions
                (session_id, player_id, game_id, started_at, ended_at,
                 duration_seconds, status)
            VALUES (?, ?, ?, NULL, ?, ?, 'ended')
            ON CONFLICT(session_id) DO UPDATE SET
                ended_at = excluded.ended_at,
                duration_seconds = excluded.duration_seconds,
                status = 'ended'
            """,
            (session_id, player_id, game_id, timestamp, duration_seconds),
        )
