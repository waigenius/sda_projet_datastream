import json, time, random
from confluent_kafka import Producer

p = Producer({'bootstrap.servers': 'kafka-1:9092,kafka-2:9092,kafka-3:9092'})
comforts = ["low","medium","high"]

def sample():
    # fake GPS around Paris
    lat = 48.85 + random.uniform(-0.05, 0.05)
    lon = 2.35 + random.uniform(-0.08, 0.08)
    lat2 = 48.85 + random.uniform(-0.05, 0.05)
    lon2 = 2.35 + random.uniform(-0.08, 0.08)
    return {
        "pickup": {"lat": lat, "lon": lon},
        "dropoff": {"lat": lat2, "lon": lon2},
        "comfort": random.choice(comforts),
        "ts": int(time.time()*1000)
    }

def on_delivery(err, msg):
    if err:
        print("Delivery failed:", err)

if __name__ == "__main__":
    for _ in range(100):
        evt = sample()
        p.produce("rides.source", json.dumps(evt), callback=on_delivery)
        p.poll(0)
        time.sleep(0.05)
    p.flush()
    print("Produced 100 events to rides.source")
