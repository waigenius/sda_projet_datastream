from __future__ import annotations
import json
import os
from datetime import datetime, timedelta
from typing import Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Consumer, Producer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_SOURCE = os.getenv("KAFKA_TOPIC_SOURCE", "source")
TOPIC_RESULT = os.getenv("KAFKA_TOPIC_RESULT", "result")

# Règles de coût (exemple simple)
PRICING = {
    "low":    {"base": 1.5, "per_km": 0.8, "per_min": 0.2},
    "medium": {"base": 2.0, "per_km": 1.1, "per_min": 0.3},
    "high":   {"base": 3.0, "per_km": 1.6, "per_min": 0.45},
}

MAX_MESSAGES = int(os.getenv("DAG1_MAX_MESSAGES", "6000"))
POLL_TIMEOUT = float(os.getenv("DAG1_POLL_TIMEOUT", "0.2"))  # sec
RUN_WINDOW_SECONDS = int(os.getenv("DAG1_RUN_WINDOW_SECONDS", "60"))  # sécurité

def compute_cost(record: Dict) -> float:
    comfort = record.get("comfort", "low")
    r = PRICING.get(comfort, PRICING["low"])
    distance = float(record.get("distance_km", 0.0))
    duration = float(record.get("duration_min", 0.0))
    return round(r["base"] + r["per_km"] * distance + r["per_min"] * duration, 2)

def process_messages(**_):
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "airflow-dag1-debug-1",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})

    consumer.subscribe([TOPIC_SOURCE])
    processed = 0
    start = datetime.utcnow()

    try:
        while processed < MAX_MESSAGES and (datetime.utcnow() - start).total_seconds() < RUN_WINDOW_SECONDS:
            msg = consumer.poll(POLL_TIMEOUT)
            if msg is None:  # no message
                continue
            if msg.error():
                print(f"[DAG1] Kafka error: {msg.error()}")
                continue
            try:
                rec = json.loads(msg.value().decode("utf-8"))
                rec["computed_cost"] = compute_cost(rec)
                rec["compute_ts"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
                producer.produce(TOPIC_RESULT, json.dumps(rec).encode("utf-8"))
                producer.poll(0)
                processed += 1
            except Exception as e:
                print(f"[DAG1] Bad message: {e}")
    finally:
        consumer.close()
        producer.flush()

    print(f"[DAG1] Processed {processed} messages")
    return processed

default_args = {
    "owner": "datastream",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="compute_cost_travel",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/2 * * * *",  # toutes les 2 min
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=5),
) as dag:

    compute_task = PythonOperator(
        task_id="consume_compute_publish",
        python_callable=process_messages,
        provide_context=True,
    )

    compute_task