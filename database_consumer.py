import json
import sqlite3
from datetime import datetime
from kafka import KafkaConsumer

# Connect to SQLite database
conn = sqlite3.connect("gaming_analytics.db")
cursor = conn.cursor()

# Create gaming events table
cursor.execute("""
CREATE TABLE IF NOT EXISTS gaming_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id TEXT,
    event_type TEXT,
    score INTEGER,
    event_timestamp TEXT,
    kafka_partition INTEGER,
    kafka_offset INTEGER,
    received_at TEXT
)
""")

conn.commit()

# Connect to Kafka
consumer = KafkaConsumer(
    "bgmi-events",
    bootstrap_servers="127.0.0.1:9092",
    group_id="bgmi-sqlite-group",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Kafka to SQLite integration started!")
print("Storing gaming events...\n")

try:
    for message in consumer:

        event = message.value

        cursor.execute("""
        INSERT INTO gaming_events
        (player_id, event_type, score, event_timestamp,
         kafka_partition, kafka_offset, received_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            event["player_id"],
            event["event_type"],
            event["score"],
            event["timestamp"],
            message.partition,
            message.offset,
            datetime.now().isoformat()
        ))

        conn.commit()

        print(
            f"Stored: {event['player_id']} | "
            f"{event['event_type']} | "
            f"Score: {event['score']}"
        )

except KeyboardInterrupt:
    print("\nDatabase consumer stopped.")

finally:
    consumer.close()
    conn.close()