import unittest
from unittest.mock import Mock, patch

from kafka.client import (
    DeliveryTracker,
    RetryableProcessingError,
    parse_event_payload,
    process_with_retries,
)
from kafka.settings import KafkaSettings


class KafkaIntegrationTests(unittest.TestCase):
    def test_default_settings_fit_a_single_local_broker(self) -> None:
        settings = KafkaSettings()

        self.assertEqual(settings.bootstrap_servers, "localhost:9092")
        self.assertEqual(settings.partitions, 3)
        self.assertEqual(settings.replication_factor, 1)
        self.assertNotEqual(settings.events_topic, settings.dead_letter_topic)

    def test_settings_reject_invalid_topics_and_retry_limits(self) -> None:
        with self.assertRaises(ValueError):
            KafkaSettings(events_topic="same", dead_letter_topic="same")
        with self.assertRaises(ValueError):
            KafkaSettings(partitions=0)
        with self.assertRaises(ValueError):
            KafkaSettings(processing_attempts=0)

    def test_parse_event_payload_requires_a_json_object(self) -> None:
        self.assertEqual(parse_event_payload(b'{"event_id":"abc"}'), {"event_id": "abc"})
        with self.assertRaises(ValueError):
            parse_event_payload(b"not-json")
        with self.assertRaises(ValueError):
            parse_event_payload(b"[]")
        with self.assertRaises(ValueError):
            parse_event_payload(None)

    def test_retryable_processing_failure_is_retried(self) -> None:
        settings = KafkaSettings(processing_attempts=3, retry_backoff_seconds=0)
        processor = Mock(
            side_effect=[RetryableProcessingError("temporary"), None]
        )

        process_with_retries({"event_id": "abc"}, processor, settings)

        self.assertEqual(processor.call_count, 2)

    def test_exhausted_processing_retries_raise(self) -> None:
        settings = KafkaSettings(processing_attempts=3, retry_backoff_seconds=0)
        processor = Mock(side_effect=RetryableProcessingError("still unavailable"))

        with self.assertRaises(RetryableProcessingError):
            process_with_retries({"event_id": "abc"}, processor, settings)

        self.assertEqual(processor.call_count, 3)

    def test_delivery_tracker_surfaces_broker_errors(self) -> None:
        tracker = DeliveryTracker()
        tracker.on_delivery(Mock(__str__=lambda _: "broker rejected event"), Mock())

        with self.assertRaisesRegex(RuntimeError, "failed delivery"):
            tracker.raise_if_failed()

    def test_delivery_tracker_accepts_success(self) -> None:
        tracker = DeliveryTracker()
        tracker.on_delivery(None, Mock())

        tracker.raise_if_failed()

    def test_retry_delay_is_applied_between_attempts(self) -> None:
        settings = KafkaSettings(processing_attempts=2, retry_backoff_seconds=0.1)
        processor = Mock(
            side_effect=[RetryableProcessingError("temporary"), None]
        )

        with patch("kafka.client.time.sleep") as sleep:
            process_with_retries({"event_id": "abc"}, processor, settings)

        sleep.assert_called_once_with(0.1)


if __name__ == "__main__":
    unittest.main()
