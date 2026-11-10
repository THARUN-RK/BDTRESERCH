import argparse
import json
from collections.abc import Sequence

from kafka.client import RejectedEventError, consume_events
from kafka.settings import KafkaSettings
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
    preprocessor = EventPreprocessor()

    def preprocess_and_display(event: dict[str, object]) -> bool:
        result = preprocessor.process(event)
        if result.status is EventStatus.INVALID:
            if result.reason is None:
                raise RuntimeError("Invalid event result is missing its validation reason")
            raise RejectedEventError(result.reason)
        if result.status is EventStatus.DUPLICATE:
            return False
        if result.event is None:
            raise RuntimeError("Valid event result is missing its normalized event")
        _display_event(result.event)
        return True

    processed, dead_lettered = consume_events(
        settings,
        processor=preprocess_and_display,
        max_messages=args.max_messages or None,
    )
    print(
        f"Accepted {processed} event(s), counted {preprocessor.stats.duplicate} "
        f"duplicate(s), rejected {preprocessor.stats.invalid} invalid event(s), "
        f"and sent {dead_lettered} message(s) to the DLQ."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
