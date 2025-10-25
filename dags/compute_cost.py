from __future__ import annotations
import json, math, time, sys, signal
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

BOOTSTRAP = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
IN_TOPIC  = "rides.source"
OUT_TOPIC = "rides.result"
GROUP_ID  = "compute_cost_v1"
BATCH_MAX = 500
IDLE_QUIT_SEC = 5

def haversine_m(p1, p2):
    R = 6371000.0
    phi1 = math.radians(p1["lat"]); phi2 = math.radians(p2["lat"])
    dphi = phi2 - phi1
    dlambda = math.radians(p2["lon"] - p1["lon"])
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2*R*math.atan2(math.sqrt(a), math.sqrt(1-a))

def compute_cost_eur(distance_m: float, comfort: str) -> float:
    base = {"low": 0.8, "medium": 1.0, "high": 1.4}.get(comfort, 1.0)
    return round(2.0 + base * (distance_m/1000.0) * 1.2, 2)

def task_compute_cost():
    from confluent_kafka import Consumer, Producer
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "session.timeout.ms": 10000,
    })
    producer = Producer({"bootstrap.servers": BOOTSTRAP})

    running = True
    def _shutdown(sig, frame):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    consumer.subscribe([IN_TOPIC])
    processed = 0
    t0_idle = time.time()
    try:
        while running and processed < BATCH_MAX:
            msg = consumer.poll(0.5)
            if msg is None:
                if time.time() - t0_idle > IDLE_QUIT_SEC and processed > 0:
                    break
                continue
            if msg.error():
                print(f"[WARN] consume error: {msg.error()}", file=sys.stderr); continue
            t0_idle = time.time()
            try:
                payload = json.loads(msg.value())
                pickup = payload["pickup"]; dropoff = payload["dropoff"]
                comfort = str(payload.get("comfort", "medium"))
                dist_m = haversine_m(pickup, dropoff)
                cost = compute_cost_eur(dist_m, comfort)
                out_evt = {
                    **payload,
                    "distance_m": round(dist_m, 2),
                    "cost_eur": cost,
                    "agent_timestamp": datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
                }
                producer.produce(OUT_TOPIC, json.dumps(out_evt).encode("utf-8"))
                processed += 1
                if processed % 100 == 0:
                    producer.flush(2)
            except Exception as e:
                print(f"[ERROR] processing failed: {e}", file=sys.stderr)
        producer.flush(5)
        print(f"[INFO] processed={processed}")
    finally:
        consumer.close()

with DAG(
    dag_id="compute_cost",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "datastream"},
) as dag:
    PythonOperator(
        task_id="compute_cost_batch",
        python_callable=task_compute_cost,
    )