from __future__ import annotations
import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Consumer, Producer

# ==== Config ====
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_SOURCE = os.getenv("KAFKA_TOPIC_SOURCE", "source")
TOPIC_RESULT = os.getenv("KAFKA_TOPIC_RESULT", "result")

GROUP_CONSUME = os.getenv("DAG1_GROUP_CONSUME", "airflow-dag1-consume")
MAX_MESSAGES = int(os.getenv("DAG1_MAX_MESSAGES", "500"))
POLL_TIMEOUT = float(os.getenv("DAG1_POLL_TIMEOUT", "0.2"))
RUN_WINDOW_SECONDS = int(os.getenv("DAG1_RUN_WINDOW_SECONDS", "30"))

# Règles de coût
PRICING = {
    "low":    {"base": 1.5, "per_km": 0.8,  "per_min": 0.2},
    "medium": {"base": 2.0, "per_km": 1.1,  "per_min": 0.3},
    "high":   {"base": 3.0, "per_km": 1.6,  "per_min": 0.45},
}

# ==== Task 1: consume from Kafka (topic source) ====
def task_consume(**_context) -> List[Dict[str, Any]]:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": GROUP_CONSUME,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([TOPIC_SOURCE])
    batch: List[Dict[str, Any]] = []
    start = datetime.utcnow()

    try:
        while len(batch) < MAX_MESSAGES and (datetime.utcnow() - start).total_seconds() < RUN_WINDOW_SECONDS:
            msg = consumer.poll(POLL_TIMEOUT)
            if msg is None:
                continue
            if msg.error():
                print(f"[ConsumKafka] Kafka error: {msg.error()}")
                continue
            try:
                rec = json.loads(msg.value().decode("utf-8"))
                batch.append(rec)
            except Exception as e:
                print(f"[ConsumKafka] Bad message: {e}")
    finally:
        consumer.close()

    print(f"[ConsumKafka] Pulled {len(batch)} messages from '{TOPIC_SOURCE}'")
    return batch  # envoyé en XCom

# ==== Task 2: compute cost ====
def compute_cost(record: Dict[str, Any]) -> float:
    comfort = record.get("comfort") or record.get("confort") or "low"
    r = PRICING.get(comfort, PRICING["low"])
    distance = float(record.get("distance_km") or record.get("distance") or 0.0)
    duration = float(record.get("duration_min") or record.get("duree") or 0.0)
    return round(r["base"] + r["per_km"] * distance + r["per_min"] * duration, 2)

def task_compute(ti, **_context) -> List[Dict[str, Any]]:
    batch: List[Dict[str, Any]] = ti.xcom_pull(task_ids="ConsumKafka")
    if not batch:
        print("[ComputCostTravel] Nothing to compute.")
        return []

    out: List[Dict[str, Any]] = []
    for rec in batch:
        try:
            rec = dict(rec)  # shallow copy
            rec["computed_cost"] = compute_cost(rec)
            rec["compute_ts"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
            out.append(rec)
        except Exception as e:
            print(f"[ComputCostTravel] Failed to compute: {e}")

    print(f"[ComputCostTravel] Computed {len(out)} / {len(batch)} messages")
    return out

# ==== Task 3: publish to Kafka (topic result) ====
def task_publish(ti, **_context) -> int:
    batch: List[Dict[str, Any]] = ti.xcom_pull(task_ids="ComputCostTravel")
    if not batch:
        print("[PublishKafka] Nothing to publish.")
        return 0

    producer = Producer({"bootstrap.servers": KAFKA_BROKER})
    sent = 0

    def _cb(err, _msg):
        if err:
            print(f"[PublishKafka] Delivery failed: {err}")

    for rec in batch:
        try:
            producer.produce(TOPIC_RESULT, json.dumps(rec).encode("utf-8"), callback=_cb)
            sent += 1
        except Exception as e:
            print(f"[PublishKafka] Produce error: {e}")
        producer.poll(0)

    producer.flush()
    print(f"[PublishKafka] Published {sent} messages to '{TOPIC_RESULT}'")
    return sent

# ==== DAG definition ====
default_args = {"owner": "datastream", "depends_on_past": False, "retries": 0}

with DAG(
    dag_id="compute_cost_v2",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/2 * * * *",   # toutes les 2 minutes
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=5),
    tags=["kafka", "compute_cost"],
) as dag:

    t1 = PythonOperator(
        task_id="ConsumKafka",
        python_callable=task_consume,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="ComputCostTravel",
        python_callable=task_compute,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id="PublishKafka",
        python_callable=task_publish,
        provide_context=True,
    )

    t1 >> t2 >> t3