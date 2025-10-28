import json
import os
import random
import time
from datetime import datetime
from confluent_kafka import Producer
import fastjsonschema

BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC_SOURCE", "source")
RATE = float(os.getenv("PRODUCER_RATE_PER_SEC", "5"))  # messages / sec
SCHEMA_PATH = os.getenv("DATA_SCHEMA_PATH", "/app/data/data_projet.json")

# Chargement du schéma Kafka (si fourni) pour valider les messages
# --- Validation toggle ---
SKIP_VALIDATION = os.getenv("PRODUCER_SKIP_VALIDATION", "false").lower() == "true"
validate = None
if not SKIP_VALIDATION and os.path.exists(SCHEMA_PATH) and SCHEMA_PATH.strip():
    try:
        with open(SCHEMA_PATH, "r") as f:
            schema = json.load(f)
        validate = fastjsonschema.compile(schema)
        print(f"[producer] Loaded schema at {SCHEMA_PATH}")
    except Exception as e:
        print(f"[producer] Schema load error (ignored): {e}")

producer = Producer({"bootstrap.servers": BROKER, "client.id": "demo-producer"})

def on_delivery(err, msg):
    if err:
        print(f"[producer] Delivery failed: {err}")
    else:
        pass  # could log offsets

def fake_event() -> dict:
    comfort = random.choice(["low", "medium", "high"])
    distance_km = round(random.uniform(0.5, 25.0), 2)
    duration_min = round(distance_km * random.uniform(1.5, 3.5), 1)
    # Coords approximatives (Paris)
    lat = round(48.8 + random.random()*0.4, 6)
    lon = round(2.2 + random.random()*0.6, 6)
    return {
        "ride_id": f"r-{int(time.time()*1000)}-{random.randint(100,999)}",
        "user_id": f"u-{random.randint(1,9999)}",
        "comfort": comfort,
        "distance_km": distance_km,
        "duration_min": duration_min,
        "pickup": {"lat": lat, "lon": lon},
        "dropoff": {"lat": lat + random.uniform(-0.02,0.02),
                    "lon": lon + random.uniform(-0.02,0.02)},
        "event_time": datetime.utcnow().isoformat(timespec="seconds") + "Z"
    }

def main():
    interval = 1.0 / RATE if RATE > 0 else 0
    print(f"[producer] Sending to {BROKER} topic={TOPIC} at ~{RATE}/s")
    while True:
        evt = fake_event()
        if validate:
            try:
                validate(evt)
            except Exception as e:
                print(f"[producer] Validation failed: {e}")
                time.sleep(interval)
                continue
        producer.produce(TOPIC, json.dumps(evt).encode("utf-8"), callback=on_delivery)
        producer.poll(0)
        time.sleep(interval)

if __name__ == "__main__":
    main()