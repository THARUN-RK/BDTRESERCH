import tempfile
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from backend.app import create_app
from database import Database


class BackendApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.database_path = Path(self.temporary_directory.name) / "api.sqlite3"
        self.database = Database(self.database_path)
        self.client = TestClient(
            create_app(
                database_path=self.database_path,
                bootstrap_servers="127.0.0.1:9092",
            )
        )

    def tearDown(self) -> None:
        self.client.close()
        self.temporary_directory.cleanup()

    def test_health_and_empty_analytics_endpoints(self) -> None:
        self.assertEqual(self.client.get("/health").json(), {
            "status": "ok",
            "database": "connected",
        })

        overview = self.client.get("/api/metrics/overview")
        self.assertEqual(overview.status_code, 200)
        self.assertEqual(overview.json()["total_processed_events"], 0)
        self.assertIsNone(overview.json()["kafka_processing_latency_ms"])

        self.assertEqual(self.client.get("/api/metrics/activity").json(), [])
        self.assertEqual(len(self.client.get("/api/metrics/daily?days=3").json()), 3)
        self.assertEqual(self.client.get("/api/leaderboard").json(), [])
        self.assertEqual(self.client.get("/api/sessions").json(), [])
        self.assertEqual(
            self.client.get("/api/data-quality").json(),
            {"valid": 0, "invalid": 0, "duplicate": 0},
        )

    def test_analytics_endpoints_return_actual_stored_records(self) -> None:
        timestamp = datetime.now(timezone.utc).isoformat()
        player_id = "player-00001"
        session_id = str(uuid.uuid4())
        events = [
            {
                "event_id": str(uuid.uuid4()),
                "event_type": "player_login",
                "timestamp": timestamp,
                "player_id": player_id,
            },
            {
                "event_id": str(uuid.uuid4()),
                "event_type": "game_start",
                "timestamp": timestamp,
                "player_id": player_id,
                "session_id": session_id,
                "game_id": "arena",
            },
            {
                "event_id": str(uuid.uuid4()),
                "event_type": "player_score",
                "timestamp": timestamp,
                "player_id": player_id,
                "session_id": session_id,
                "game_id": "arena",
                "score": 250,
            },
        ]
        for event in events:
            self.database.store_event(event)

        overview = self.client.get("/api/metrics/overview").json()
        self.assertEqual(overview["total_players"], 1)
        self.assertEqual(overview["active_players"], 1)
        self.assertEqual(overview["total_sessions"], 1)
        self.assertEqual(overview["average_score"], 250.0)
        self.assertEqual(overview["highest_score"], 250)

        leaderboard = self.client.get("/api/leaderboard").json()
        self.assertEqual(leaderboard[0]["player_id"], player_id)
        self.assertEqual(leaderboard[0]["total_score"], 250)
        self.assertEqual(
            self.client.get("/api/sessions").json()[0]["session_id"], session_id
        )
        self.assertTrue(self.client.get("/api/metrics/activity").json())

    def test_kafka_status_reports_reachability_and_unmeasured_lag(self) -> None:
        status = self.client.get("/api/kafka/status")

        self.assertEqual(status.status_code, 200)
        self.assertIsInstance(status.json()["broker_reachable"], bool)
        self.assertIsNone(status.json()["consumer_lag"])
        self.assertIn("not instrumented", status.json()["consumer_lag_note"])

    def test_rejects_out_of_range_query_parameters(self) -> None:
        self.assertEqual(
            self.client.get("/api/metrics/activity?minutes=0").status_code, 422
        )
        self.assertEqual(
            self.client.get("/api/leaderboard?limit=101").status_code, 422
        )


if __name__ == "__main__":
    unittest.main()
