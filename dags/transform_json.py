from __future__ import annotations
import json, sys, time, signal
from datetime import datetime
from typing import List, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator

BOOTSTRAP  = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
IN_TOPIC   = "rides.result"
ES_HOST    = "http://elasticsearch:9200"
ES_INDEX   = "rides-000001"
GROUP_ID   = "transform_json_v2"

# Réduire la pression:
BATCH_MAX  = 200      # max d'évts traités par run
IDLE_QUIT  = 3        # stop si pas de données pendant 3s
CHUNK_SIZE = 50       # taille des paquets envoyés à ES
REQ_TIMEOUT = 30

def to_actions(batch: List[Dict]):
    for doc in batch:
        if "agent_timestamp" not in doc:
            doc["agent_timestamp"] = datetime.utcnow().isoformat(timespec="milliseconds") + "Z"
        yield {"_index": ES_INDEX, "_source": doc}

def task_to_elasticsearch():
    from confluent_kafka import Consumer
    from elasticsearch import Elasticsearch, helpers

    es = Elasticsearch(ES_HOST, request_timeout=REQ_TIMEOUT)
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "session.timeout.ms": 10000,
    })

    running = True
    def _shutdown(sig, frame):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    consumer.subscribe([IN_TOPIC])

    buf: List[Dict] = []
    processed = 0
    last_data_ts = time.time()

    try:
        while running and processed < BATCH_MAX:
            msg = consumer.poll(0.2)
            if msg is None:
                if buf:
                    helpers.bulk(
                        es, to_actions(buf),
                        chunk_size=CHUNK_SIZE, request_timeout=REQ_TIMEOUT,
                        raise_on_error=False, refresh=False
                    )
                    buf.clear()
                if processed > 0 and (time.time() - last_data_ts) > IDLE_QUIT:
                    break
                continue

            if msg.error():
                print(f"[WARN] consume error: {msg.error()}", file=sys.stderr); continue

            last_data_ts = time.time()
            try:
                doc = json.loads(msg.value())
                buf.append(doc)
                processed += 1
                if len(buf) >= CHUNK_SIZE:
                    helpers.bulk(
                        es, to_actions(buf),
                        chunk_size=CHUNK_SIZE, request_timeout=REQ_TIMEOUT,
                        raise_on_error=False, refresh=False
                    )
                    buf.clear()
            except Exception as e:
                print(f"[ERROR] parse/index failed: {e}", file=sys.stderr)

        if buf:
            helpers.bulk(
                es, to_actions(buf),
                chunk_size=CHUNK_SIZE, request_timeout=REQ_TIMEOUT,
                raise_on_error=False, refresh=False
            )
            buf.clear()
        print(f"[INFO] indexed={processed}")
    finally:
        consumer.close()

with DAG(
    dag_id="transform_json",
    start_date=datetime(2024,1,1),
    schedule=None,
    catchup=False,
    default_args={"owner": "datastream"},
) as dag:
    PythonOperator(
        task_id="kafka_to_elasticsearch",
        python_callable=task_to_elasticsearch,
    )