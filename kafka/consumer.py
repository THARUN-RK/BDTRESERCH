import argparse
import json
from collections.abc import Sequence

from kafka.client import RejectedEventError, consume_events
from kafka.settings import KafkaSettings
from database import Database, StoreStatus
from preprocessing import EventPreprocessor, EventStatus


def _display_event(event: dict[str, object]) -> None:
    print(json.dumps(event, separators=(",", ":"), ensure_ascii=False))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Consume and display gaming events from Apache Kafka."
    )
    parser.add_argument(
        "--bootstrap-servers", default="localhost:9092", help="Kafka broker address"
    )
    parser.add_argument("--topic", default="gaming-events", help="events topic")
    parser.add_argument(
        "--group-id", default="gaming-analytics", help="Kafka consumer group"
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="stop after this many messages; 0 runs until Ctrl+C",
    )
    args = parser.parse_args(argv)
    if args.max_messages < 0:
        parser.error("--max-messages must be zero or greater")

    settings = KafkaSettings(
        bootstrap_servers=args.bootstrap_servers,
        events_topic=args.topic,
        consumer_group=args.group_id,
    )
    database = Database()
    preprocessor = EventPreprocessor()

    def preprocess_and_display(event: dict[str, object]) -> bool:
        result = preprocessor.process(event)
        if result.status is EventStatus.INVALID:
            if result.reason is None:
                raise RuntimeError("Invalid event result is missing its validation reason")
            database.record_invalid_event(
                json.dumps(event, separators=(",", ":"), ensure_ascii=False),
                result.reason,
                event_id=event.get("event_id")
                if isinstance(event.get("event_id"), str)
                else None,
            )
            raise RejectedEventError(result.reason)
        if result.status is EventStatus.DUPLICATE:
            database.record_duplicate_event(event)
            return False
        if result.event is None:
            raise RuntimeError("Valid event result is missing its normalized event")
        store_status = database.store_event(result.event)
        if store_status is StoreStatus.DUPLICATE:
            return False
        _display_event(result.event)
        return True

    def record_unparseable_message(payload: bytes | None, reason: str) -> None:
        raw_payload = (
            payload.decode("utf-8", errors="replace") if payload is not None else ""
        )
        database.record_invalid_event(raw_payload, reason)

    processed, dead_lettered = consume_events(
        settings,
        processor=preprocess_and_display,
        max_messages=args.max_messages or None,
        on_invalid_message=record_unparseable_message,
    )
    quality_counts = database.quality_counts()
    print(
        f"Accepted {processed} new event(s) in this run and sent {dead_lettered} "
        f"message(s) to the DLQ. Stored quality totals: "
        f"{quality_counts['valid']} valid, {quality_counts['invalid']} invalid, "
        f"{quality_counts['duplicate']} duplicate."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
