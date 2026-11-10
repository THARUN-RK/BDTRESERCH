import argparse
import math
from collections.abc import Sequence

from event_generator.generator import GamingEventGenerator
from kafka.client import (
    DeliveryTracker,
    create_producer,
    ensure_topics,
    flush_producer,
    publish_event,
)
from kafka.settings import KafkaSettings


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate gaming events and publish them to Apache Kafka."
    )
    parser.add_argument("--count", type=int, default=1000, help="events; 0 runs forever")
    parser.add_argument("--rate", type=float, default=10.0, help="events per second")
    parser.add_argument("--players", type=int, default=100, help="simulated players")
    parser.add_argument("--seed", type=int, default=None, help="random seed")
    parser.add_argument(
        "--bootstrap-servers", default="localhost:9092", help="Kafka broker address"
    )
    parser.add_argument("--topic", default="gaming-events", help="events topic")
    parser.add_argument("--partitions", type=int, default=3, help="topic partitions")
    args = parser.parse_args(argv)
    if args.count < 0:
        parser.error("--count must be zero or greater")
    if not math.isfinite(args.rate) or args.rate <= 0:
        parser.error("--rate must be greater than zero")
    if args.players < 1:
        parser.error("--players must be at least 1")
    if args.partitions < 1:
        parser.error("--partitions must be at least 1")

    settings = KafkaSettings(
        bootstrap_servers=args.bootstrap_servers,
        events_topic=args.topic,
        partitions=args.partitions,
    )
    ensure_topics(settings)
    producer = create_producer(settings)
    delivery_tracker = DeliveryTracker()
    generator = GamingEventGenerator(players=args.players, seed=args.seed)

    try:
        generated = generator.run(
            count=args.count,
            events_per_second=args.rate,
            sink=lambda event: publish_event(
                producer, event, settings, delivery_tracker
            ),
        )
    finally:
        flush_producer(producer, delivery_tracker)

    print(
        f"Published {generated} event(s) to '{settings.events_topic}' "
        f"(at {args.rate:g} events/second)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
