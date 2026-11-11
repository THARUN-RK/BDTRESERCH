import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="127.0.0.1:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "bgmi-events"

players = ["Player101", "Player102", "Player103", "Player104"]

print("BGMI Gaming Event Generator Started...")

try:
    while True:
        event = {
            "player_id": random.choice(players),
            "event_type": random.choice([
                "kill",
                "death",
                "match_start",
                "match_end",
                "loot"
            ]),
            "score": random.randint(0, 100),
            "timestamp": datetime.now().isoformat()
        }

        producer.send(topic, value=event)
        print("Event sent:", event)

        time.sleep(1)

except KeyboardInterrupt:
    print("Producer stopped.")

finally:
    producer.flush()
    producer.close()