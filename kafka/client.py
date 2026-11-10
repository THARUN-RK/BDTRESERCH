import base64
import json
import time
from collections.abc import Callable

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from kafka.settings import KafkaSettings

EventProcessor = Callable[[dict[str, object]], None]


class RetryableProcessingError(Exception):
    """Signal a transient event-processing failure that should be retried."""


class DeliveryTracker:
    def __init__(self) -> None:
        self.errors: list[str] = []

    def on_delivery(self, error: KafkaError | None, message: Message) -> None:
        if error is not None:
            self.errors.append(str(error))

    def raise_if_failed(self) -> None:
        if self.errors:
            details = "; ".join(self.errors[:5])
            raise RuntimeError(
                f"{len(self.errors)} Kafka message(s) failed delivery: {details}"
            )


def ensure_topics(settings: KafkaSettings) -> None:
    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    cluster = admin.list_topics(timeout=10)
    existing = cluster.topics
    desired = (settings.events_topic, settings.dead_letter_topic)
    missing = [
        NewTopic(
            topic,
            num_partitions=settings.partitions,
            replication_factor=settings.replication_factor,
        )
        for topic in desired
        if topic not in existing
    ]

    futures = admin.create_topics(missing) if missing else {}
    for topic, future in futures.items():
        future.result(timeout=10)


def create_producer(settings: KafkaSettings) -> Producer:
    return Producer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "retries": 5,
            "retry.backoff.ms": 250,
            "message.timeout.ms": 30000,
            "queue.buffering.max.messages": 10000,
            "queue.buffering.max.kbytes": 16384,
        }
    )


def create_consumer(
    settings: KafkaSettings, group_id: str | None = None
) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": group_id or settings.consumer_group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "queued.max.messages.kbytes": 8192,
        }
    )


def publish_event(
    producer: Producer,
    event: dict[str, object],
    settings: KafkaSettings,
    delivery_tracker: DeliveryTracker,
) -> None:
    player_id = event.get("player_id")
    if not isinstance(player_id, str) or not player_id:
        raise ValueError("event must contain a non-empty string player_id")

    payload = json.dumps(event, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )
    for attempt in range(5):
        try:
            producer.produce(
                settings.events_topic,
                key=player_id.encode("utf-8"),
                value=payload,
                on_delivery=delivery_tracker.on_delivery,
            )
            break
        except BufferError:
            if attempt == 4:
                raise
            producer.poll(0.5)
    producer.poll(0)


def flush_producer(
    producer: Producer, delivery_tracker: DeliveryTracker, timeout: float = 30.0
) -> None:
    remaining = producer.flush(timeout=timeout)
    if remaining:
        raise TimeoutError(
            f"{remaining} Kafka message(s) remained undelivered after {timeout:g}s"
        )
    delivery_tracker.raise_if_failed()


def parse_event_payload(payload: bytes | None) -> dict[str, object]:
    if payload is None:
        raise ValueError("Kafka message has no value")

    event = json.loads(payload.decode("utf-8"))
    if not isinstance(event, dict):
        raise ValueError("Kafka event must be a JSON object")
    return event


def process_with_retries(
    event: dict[str, object],
    processor: EventProcessor,
    settings: KafkaSettings,
) -> None:
    for attempt in range(settings.processing_attempts):
        try:
            processor(event)
            return
        except RetryableProcessingError:
            if attempt + 1 == settings.processing_attempts:
                raise
            if settings.retry_backoff_seconds:
                time.sleep(settings.retry_backoff_seconds)


def send_to_dead_letter(
    producer: Producer, message: Message, error: Exception, settings: KafkaSettings
) -> None:
    raw_value = message.value()
    dead_letter_event = {
        "source_topic": message.topic(),
        "source_partition": message.partition(),
        "source_offset": message.offset(),
        "error": str(error)[:1000],
        "raw_value_base64": base64.b64encode(raw_value).decode("ascii")
        if raw_value is not None
        else None,
    }
    delivery_errors: list[KafkaError] = []

    def on_delivery(delivery_error: KafkaError | None, _: Message) -> None:
        if delivery_error is not None:
            delivery_errors.append(delivery_error)

    producer.produce(
        settings.dead_letter_topic,
        key=message.key(),
        value=json.dumps(dead_letter_event, separators=(",", ":")).encode("utf-8"),
        on_delivery=on_delivery,
    )
    remaining = producer.flush(timeout=10)
    if remaining:
        raise TimeoutError("Dead-letter event was not delivered within 10 seconds")
    if not delivery_errors:
        raise RuntimeError("Kafka did not report dead-letter delivery status")
    if delivery_errors[0] is not None:
        raise KafkaException(delivery_errors[0])


def consume_events(
    settings: KafkaSettings,
    processor: EventProcessor,
    max_messages: int | None = None,
    group_id: str | None = None,
) -> tuple[int, int]:
    if max_messages is not None and max_messages < 1:
        raise ValueError("max_messages must be at least 1 when provided")

    consumer = create_consumer(settings, group_id=group_id)
    dead_letter_producer = create_producer(settings)
    processed = 0
    dead_lettered = 0
    received = 0
    consumer.subscribe([settings.events_topic])

    try:
        while max_messages is None or received < max_messages:
            message = consumer.poll(timeout=1.0)
            if message is None:
                continue
            error = message.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(error)

            received += 1
            try:
                event = parse_event_payload(message.value())
            except (UnicodeDecodeError, ValueError) as parse_error:
                send_to_dead_letter(dead_letter_producer, message, parse_error, settings)
                dead_lettered += 1
            else:
                try:
                    process_with_retries(event, processor, settings)
                except RetryableProcessingError as processing_error:
                    send_to_dead_letter(
                        dead_letter_producer, message, processing_error, settings
                    )
                    dead_lettered += 1
                else:
                    processed += 1

            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()

    return processed, dead_lettered
