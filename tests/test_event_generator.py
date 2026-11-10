import json
import unittest
from datetime import datetime

from event_generator.generator import EVENT_WEIGHTS, GamingEventGenerator


class GamingEventGeneratorTests(unittest.TestCase):
    def test_generates_unique_timestamped_events(self) -> None:
        generator = GamingEventGenerator(players=25, seed=7)
        events = [generator.generate_event() for _ in range(500)]

        self.assertEqual(len(events), len({event["event_id"] for event in events}))
        for event in events:
            self.assertIn(event["event_type"], EVENT_WEIGHTS)
            self.assertIn(event["player_id"], {
                f"player-{number:05d}" for number in range(1, 26)
            })
            parsed_timestamp = datetime.fromisoformat(event["timestamp"])
            self.assertIsNotNone(parsed_timestamp.tzinfo)

    def test_emits_all_supported_event_types(self) -> None:
        generator = GamingEventGenerator(players=50, seed=17)
        events = [generator.generate_event() for _ in range(2000)]

        self.assertEqual(set(EVENT_WEIGHTS), {event["event_type"] for event in events})

    def test_session_events_keep_ids_and_duration(self) -> None:
        generator = GamingEventGenerator(players=10, seed=23)
        started_sessions: set[str] = set()

        for _ in range(2000):
            event = generator.generate_event()
            if event["event_type"] == "game_start":
                started_sessions.add(event["session_id"])
            elif event["event_type"] == "game_end":
                self.assertIn(event["session_id"], started_sessions)
                self.assertGreaterEqual(event["session_duration_seconds"], 0)
            elif "session_duration_seconds" in event:
                self.assertGreaterEqual(event["session_duration_seconds"], 0)

    def test_run_emits_requested_count_as_json_compatible_events(self) -> None:
        generator = GamingEventGenerator(players=5, seed=3)
        events: list[dict[str, object]] = []

        generated = generator.run(
            count=5,
            events_per_second=100_000,
            sink=events.append,
        )

        self.assertEqual(generated, 5)
        self.assertEqual(len(events), 5)
        for event in events:
            json.dumps(event)

    def test_rejects_invalid_configuration(self) -> None:
        with self.assertRaises(ValueError):
            GamingEventGenerator(players=0)
        with self.assertRaises(ValueError):
            GamingEventGenerator().run(count=-1, events_per_second=1, sink=lambda _: None)
        with self.assertRaises(ValueError):
            GamingEventGenerator().run(count=1, events_per_second=0, sink=lambda _: None)
        with self.assertRaises(ValueError):
            GamingEventGenerator().run(
                count=1, events_per_second=float("inf"), sink=lambda _: None
            )


if __name__ == "__main__":
    unittest.main()
