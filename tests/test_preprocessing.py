import copy
import unittest
import uuid
from datetime import datetime, timezone

from event_generator.generator import GamingEventGenerator
from preprocessing import EventPreprocessor, EventStatus


class EventPreprocessorTests(unittest.TestCase):
    def test_accepts_and_normalizes_generated_event(self) -> None:
        generator = GamingEventGenerator(players=10, seed=18)
        preprocessor = EventPreprocessor()
        event = generator.generate_event()

        result = preprocessor.process(event)

        self.assertEqual(result.status, EventStatus.VALID)
        self.assertEqual(result.event["event_id"], event["event_id"])
        self.assertEqual(result.event["player_id"], event["player_id"])
        self.assertEqual(preprocessor.stats.valid, 1)

    def test_accepts_all_event_types_emitted_by_generator(self) -> None:
        generator = GamingEventGenerator(players=50, seed=17)
        preprocessor = EventPreprocessor()

        results = [
            preprocessor.process(generator.generate_event()) for _ in range(2000)
        ]

        self.assertTrue(all(result.status is EventStatus.VALID for result in results))
        self.assertEqual(preprocessor.stats.valid, len(results))

    def test_duplicate_event_id_is_counted_and_not_accepted_twice(self) -> None:
        preprocessor = EventPreprocessor()
        event = GamingEventGenerator(seed=2).generate_event()

        first = preprocessor.process(event)
        second = preprocessor.process(event)

        self.assertEqual(first.status, EventStatus.VALID)
        self.assertEqual(second.status, EventStatus.DUPLICATE)
        self.assertEqual(preprocessor.stats.valid, 1)
        self.assertEqual(preprocessor.stats.duplicate, 1)
        self.assertEqual(preprocessor.stats.total, 2)

    def test_rejects_non_object_and_missing_required_fields(self) -> None:
        preprocessor = EventPreprocessor()

        non_object = preprocessor.process([])
        missing_fields = preprocessor.process({"event_id": str(uuid.uuid4())})

        self.assertEqual(non_object.status, EventStatus.INVALID)
        self.assertEqual(missing_fields.status, EventStatus.INVALID)
        self.assertEqual(preprocessor.stats.invalid, 2)

    def test_rejects_invalid_id_type_timestamp_and_event_type(self) -> None:
        base = self._valid_login_event()
        cases = []
        invalid_id = copy.deepcopy(base)
        invalid_id["event_id"] = "not-a-uuid"
        cases.append(invalid_id)
        invalid_timestamp = copy.deepcopy(base)
        invalid_timestamp["timestamp"] = "2026-99-42T10:00:00"
        cases.append(invalid_timestamp)
        naive_timestamp = copy.deepcopy(base)
        naive_timestamp["timestamp"] = "2026-11-10T10:00:00"
        cases.append(naive_timestamp)
        unknown_type = copy.deepcopy(base)
        unknown_type["event_type"] = "unknown"
        cases.append(unknown_type)
        numeric_player = copy.deepcopy(base)
        numeric_player["player_id"] = 42
        cases.append(numeric_player)

        preprocessor = EventPreprocessor()
        results = [preprocessor.process(event) for event in cases]

        self.assertTrue(all(result.status is EventStatus.INVALID for result in results))
        self.assertEqual(preprocessor.stats.invalid, len(cases))

    def test_rejects_invalid_score_and_non_finite_duration(self) -> None:
        session_id = str(uuid.uuid4())
        common = {
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "player_id": "player-00001",
            "session_id": session_id,
            "game_id": "arena",
        }
        invalid_score = {
            **common,
            "event_type": "player_score",
            "score": True,
        }
        invalid_duration = {
            **common,
            "event_id": str(uuid.uuid4()),
            "event_type": "game_end",
            "session_duration_seconds": float("nan"),
        }
        preprocessor = EventPreprocessor()

        self.assertEqual(
            preprocessor.process(invalid_score).status, EventStatus.INVALID
        )
        self.assertEqual(
            preprocessor.process(invalid_duration).status, EventStatus.INVALID
        )

    def test_rejects_missing_game_end_duration_and_orphan_logout_details(self) -> None:
        common = {
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "player_id": "player-00001",
            "session_id": str(uuid.uuid4()),
            "game_id": "arena",
        }
        missing_duration = {**common, "event_type": "game_end"}
        orphan_logout = {
            **common,
            "event_id": str(uuid.uuid4()),
            "event_type": "player_logout",
        }
        orphan_logout.pop("session_id")
        preprocessor = EventPreprocessor()

        self.assertEqual(
            preprocessor.process(missing_duration).status, EventStatus.INVALID
        )
        self.assertEqual(
            preprocessor.process(orphan_logout).status, EventStatus.INVALID
        )

    def test_normalizes_timestamp_to_utc_without_mutating_input(self) -> None:
        event = self._valid_login_event()
        event["timestamp"] = "2026-11-10T14:00:00+05:30"
        original = copy.deepcopy(event)

        result = EventPreprocessor().process(event)

        self.assertEqual(result.status, EventStatus.VALID)
        self.assertEqual(result.event["timestamp"], "2026-11-10T08:30:00+00:00")
        self.assertEqual(event, original)

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
