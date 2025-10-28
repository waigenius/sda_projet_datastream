from __future__ import annotations
import json
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Consumer
from elasticsearch import Elasticsearch, helpers
import pandas as pd

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_RESULT = os.getenv("KAFKA_TOPIC_RESULT", "result")
ELASTIC_URL = os.getenv("ELASTIC_URL", "http://elasticsearch:9200")
EXPORT_DIR = Path(os.getenv("EXPORT_DIR", "/exports"))

MAX_MESSAGES = int(os.getenv("DAG2_MAX_MESSAGES", "6000"))
POLL_TIMEOUT = float(os.getenv("DAG2_POLL_TIMEOUT", "0.2"))
RUN_WINDOW_SECONDS = int(os.getenv("DAG2_RUN_WINDOW_SECONDS", "60"))
ES_INDEX_PREFIX = os.getenv("ES_INDEX_PREFIX", "trips")

def flatten(d: Dict[str, Any], prefix: str = "", sep: str = "_") -> Dict[str, Any]:
    out = {}
    for k, v in d.items():
        key = f"{prefix}{sep}{k}" if prefix else k
        if isinstance(v, dict):
            out.update(flatten(v, key, sep))
        else:
            out[key] = v
    return out

def consume_batch(**_):
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "airflow-dag2-debug-1",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([TOPIC_RESULT])

    docs = []
    processed = 0
    start = datetime.utcnow()

    try:
        while processed < MAX_MESSAGES and (datetime.utcnow() - start).total_seconds() < RUN_WINDOW_SECONDS:
            msg = consumer.poll(POLL_TIMEOUT)
            if msg is None:
                continue
            if msg.error():
                print(f"[DAG2] Kafka error: {msg.error()}")
                continue
            try:
                rec = json.loads(msg.value().decode("utf-8"))
                rec["agent_timestamp"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
                flat = flatten(rec)
                docs.append(flat)
                processed += 1
            except Exception as e:
                print(f"[DAG2] Bad message: {e}")
    finally:
        consumer.close()

    print(f"[DAG2] Collected {processed} docs")
    return docs

def index_and_export(ti, **_):
    docs = ti.xcom_pull(task_ids="consume")
    if not docs:
        print("[DAG2] Nothing to index/export")
        return 0

    # Elasticsearch
    es = Elasticsearch(ELASTIC_URL)
    today = datetime.utcnow().strftime("%Y.%m.%d")
    index_name = f"{ES_INDEX_PREFIX}-{today}"

    actions = ({
        "_index": index_name,
        "_source": d
    } for d in docs)

    helpers.bulk(es, actions, chunk_size=500)
    print(f"[DAG2] Indexed {len(docs)} docs to {index_name}")

    # Export parquet partitionné par date/heure
    now = datetime.utcnow()
    out_dir = EXPORT_DIR / f"year={now:%Y}/month={now:%m}/day={now:%d}/hour={now:%H}"
    out_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(docs)
    file_path = out_dir / f"part-{int(now.timestamp())}.parquet"
    df.to_parquet(file_path, index=False)
    print(f"[DAG2] Exported {len(df)} rows to {file_path}")

    return len(docs)

default_args = {
    "owner": "datastream",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="transform_to_es_and_export",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/3 * * * *",  # toutes les 3 min
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=5),
) as dag:

    consume = PythonOperator(
        task_id="consume",
        python_callable=consume_batch,
        provide_context=True,
    )

    index_export = PythonOperator(
        task_id="index_and_export",
        python_callable=index_and_export,
        provide_context=True,
    )

    consume >> index_export