import json
import sqlite3
import tempfile
import unittest
import uuid
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

from database import Database, StoreStatus


class DatabaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.database_path = Path(self.temporary_directory.name) / "nested" / "test.sqlite3"
        self.database = Database(self.database_path)

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def test_initializes_schema_and_creates_parent_directory(self) -> None:
        counts = self.database.table_counts()

        self.assertTrue(self.database_path.exists())
        self.assertEqual(
            counts,
            {
                "players": 0,
                "gaming_sessions": 0,
                "player_scores": 0,
                "transactions": 0,
                "processed_events": 0,
                "event_quality": 0,
            },
        )

    def test_persists_event_and_prevents_duplicate_event_id(self) -> None:
        event = self._valid_login_event()

        first = self.database.store_event(event)
        second = self.database.store_event(event)

        self.assertEqual(first, StoreStatus.STORED)
        self.assertEqual(second, StoreStatus.DUPLICATE)
        self.assertEqual(self.database.table_counts()["processed_events"], 1)
        self.assertEqual(self.database.quality_counts(), {
            "valid": 1,
            "invalid": 0,
            "duplicate": 1,
        })

    def test_persists_sessions_scores_transactions_and_player_state(self) -> None:
        player_id = "player-00001"
        session_id = str(uuid.uuid4())
        start = datetime(2026, 11, 10, 8, 0, tzinfo=timezone.utc)
        events = [
            {
                "event_id": str(uuid.uuid4()),
                "event_type": "player_login",
                "timestamp": start.isoformat(),
                "player_id": player_id,
            },
            {
                "event_id": str(uuid.uuid4()),
                "event_type": "game_start",
                "timestamp": (start + timedelta(minutes=1)).isoformat(),
                "player_id": player_id,
                "session_id": session_id,
                "game_id": "arena",
            },
            {
                "event_id": str(uuid.uuid4()),
                "event_type": "player_score",
                "timestamp": (start + timedelta(minutes=5)).isoformat(),
                "player_id": player_id,
                "session_id": session_id,
                "game_id": "arena",
                "score": 250,
            },
            {
                "event_id": str(uuid.uuid4()),
                "event_type": "transaction",
                "timestamp": (start + timedelta(minutes=6)).isoformat(),
                "player_id": player_id,
                "transaction_id": str(uuid.uuid4()),
                "amount_cents": 499,
                "currency": "USD",
            },
            {
                "event_id": str(uuid.uuid4()),
                "event_type": "game_end",
                "timestamp": (start + timedelta(minutes=10)).isoformat(),
                "player_id": player_id,
                "session_id": session_id,
                "game_id": "arena",
                "session_duration_seconds": 540.0,
            },
            {
                "event_id": str(uuid.uuid4()),
                "event_type": "player_logout",
                "timestamp": (start + timedelta(minutes=10)).isoformat(),
                "player_id": player_id,
            },
        ]
        for event in events:
            self.database.store_event(event)

        counts = self.database.table_counts()
        self.assertEqual(counts["players"], 1)
        self.assertEqual(counts["gaming_sessions"], 1)
        self.assertEqual(counts["player_scores"], 1)
        self.assertEqual(counts["transactions"], 1)
        self.assertEqual(counts["processed_events"], len(events))
        self.assertEqual(counts["event_quality"], len(events))

    def test_records_invalid_raw_payloads_with_reasons(self) -> None:
        raw_payload = '{"event_id":"broken","score":"not-a-number"}'

        self.database.record_invalid_event(
            raw_payload=raw_payload,
            reason="score must be a non-negative integer",
            event_id="broken",
        )

        self.assertEqual(
            self.database.quality_counts(),
            {"valid": 0, "invalid": 1, "duplicate": 0},
        )

    def test_normalizes_player_online_state_from_login_and_logout(self) -> None:
        login = self._valid_login_event()
        logout = {
            **login,
            "event_id": str(uuid.uuid4()),
            "event_type": "player_logout",
            "timestamp": "2026-11-10T09:00:00+00:00",
        }
        self.database.store_event(login)
        self.database.store_event(logout)

        with closing(sqlite3.connect(self.database_path)) as connection:
            player = connection.execute(
                "SELECT is_online FROM players WHERE player_id = ?",
                (login["player_id"],),
            ).fetchone()

        self.assertEqual(player[0], 0)

    def test_rejects_non_json_finite_values_before_storage(self) -> None:
        event = self._valid_login_event()
        event["unexpected"] = float("nan")

        with self.assertRaises(ValueError):
            self.database.store_event(event)

        self.assertEqual(self.database.table_counts()["processed_events"], 0)

    @staticmethod
    def _valid_login_event() -> dict[str, object]:
        return {
            "event_id": str(uuid.uuid4()),
            "event_type": "player_login",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "player_id": "player-00001",
        }


if __name__ == "__main__":
    unittest.main()
