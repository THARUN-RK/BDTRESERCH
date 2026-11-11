import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "bgmi-events",
    bootstrap_servers="127.0.0.1:9092",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="bgmi-analytics-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("BGMI Kafka Consumer Started...")
print("Waiting for gaming events...\n")

try:
    for message in consumer:
        event = message.value

        print("--------------------------------")
        print("PLAYER ID :", event["player_id"])
        print("EVENT     :", event["event_type"])
        print("SCORE     :", event["score"])
        print("TIMESTAMP :", event["timestamp"])
        print("PARTITION :", message.partition)
        print("--------------------------------")

except KeyboardInterrupt:
    print("\nConsumer stopped.")

finally:
    consumer.close()