from __future__ import annotations
import json
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Consumer
from elasticsearch import Elasticsearch, helpers
import pandas as pd

# --- Optionnel : upload GCS si lib installée + bucket configuré ---
try:
    from google.cloud import storage  # type: ignore
except Exception:  # pas installé dans l'image par défaut → on ignore
    storage = None  # type: ignore

# =======================
#       CONFIG
# =======================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_RESULT = os.getenv("KAFKA_TOPIC_RESULT", "result")

GROUP_CONSUME = os.getenv("DAG2_GROUP_CONSUME", "airflow-dag2-consume")
MAX_MESSAGES = int(os.getenv("DAG2_MAX_MESSAGES", "1000"))
POLL_TIMEOUT = float(os.getenv("DAG2_POLL_TIMEOUT", "0.2"))
RUN_WINDOW_SECONDS = int(os.getenv("DAG2_RUN_WINDOW_SECONDS", "30"))

ELASTIC_URL = os.getenv("ELASTIC_URL", "http://elasticsearch:9200")
ES_INDEX_PREFIX = os.getenv("ES_INDEX_PREFIX", "trips")

EXPORT_DIR = Path(os.getenv("EXPORT_DIR", "/exports"))
# Upload GCS optionnel : nécessite d'avoir installé google-cloud-storage + credentials
GCS_BUCKET = os.getenv("GCS_BUCKET", "").strip()
GCS_PREFIX = os.getenv("GCS_PREFIX", "raw/trips").strip()  # préfixe dossier dans le bucket
ENABLE_GCS_UPLOAD = os.getenv("ENABLE_GCS_UPLOAD", "false").lower() == "true"

# =======================
#    HELPERS
# =======================
def flatten(d: Dict[str, Any], prefix: str = "", sep: str = "_") -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in d.items():
        key = f"{prefix}{sep}{k}" if prefix else k
        if isinstance(v, dict):
            out.update(flatten(v, key, sep))
        else:
            out[key] = v
    return out

def _gcs_upload_if_enabled(local_file: Path, gcs_uri_path: str) -> Optional[str]:
    """
    Upload vers GCS si:
      - ENABLE_GCS_UPLOAD = true
      - GCS_BUCKET non vide
      - lib google-cloud-storage dispo
      - variables d'auth GCP configurées (GOOGLE_APPLICATION_CREDENTIALS, etc.)
    Retourne l'URI gs://... si upload fait, sinon None.
    """
    if not ENABLE_GCS_UPLOAD or not GCS_BUCKET or storage is None:
        return None
    try:
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET)
        blob = bucket.blob(gcs_uri_path)
        blob.upload_from_filename(str(local_file))
        return f"gs://{GCS_BUCKET}/{gcs_uri_path}"
    except Exception as e:
        print(f"[PutGCP] GCS upload skipped/failed: {e}")
        return None

# =======================
#     TASKS
# =======================
def task_consume(**_) -> List[Dict[str, Any]]:
    """
    Lit un batch sur 'result' et déballe les enveloppes {"data":[...]}.
    """
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": GROUP_CONSUME,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([TOPIC_RESULT])

    docs: List[Dict[str, Any]] = []
    start = datetime.utcnow()

    try:
        while len(docs) < MAX_MESSAGES and (datetime.utcnow() - start).total_seconds() < RUN_WINDOW_SECONDS:
            msg = consumer.poll(POLL_TIMEOUT)
            if msg is None:
                continue
            if msg.error():
                print(f"[ConsumKafka] Kafka error: {msg.error()}")
                continue
            try:
                obj = json.loads(msg.value().decode("utf-8"))
                # ❶ déballage {"data":[…]}
                if isinstance(obj, dict) and "data" in obj and isinstance(obj["data"], list):
                    docs.extend(obj["data"])
                # ❷ tolère un document unique
                elif isinstance(obj, dict):
                    docs.append(obj)
            except Exception as e:
                print(f"[ConsumKafka] Bad message: {e}")
    finally:
        consumer.close()

    print(f"[ConsumKafka] Pulled {len(docs)} documents after unwrapping.")
    return docs

def task_transform(ti, **_) -> List[Dict[str, Any]]:
    raw_docs: List[Dict[str, Any]] = ti.xcom_pull(task_ids="ConsumKafka") or []
    if not raw_docs:
        print("[TransformJson] Nothing to transform.")
        return []

    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    out: List[Dict[str, Any]] = []
    for d in raw_docs:
        try:
            d = dict(d)  # copy
            d["agent_timestamp"] = ts
            flat = flatten(d)
            # harmonisation minimale FR/EN des champs (optionnel)
            if "pickup_lat" in flat and "pickup_lon" in flat:
                pass
            elif "locationClient_lat" in flat and "locationClient_lon" in flat:
                flat["pickup_lat"] = flat.get("locationClient_lat")
                flat["pickup_lon"] = flat.get("locationClient_lon")
            if "dropoff_lat" not in flat and "locationDriver_lat" in flat:
                flat["dropoff_lat"]  = flat.get("locationDriver_lat")
                flat["dropoff_lon"]  = flat.get("locationDriver_lon")
            out.append(flat)
        except Exception as e:
            print(f"[TransformJson] Transform failed: {e}")

    print(f"[TransformJson] Transformed {len(out)} docs")
    return out

def task_put_es(ti, **_) -> int:
    """Indexe dans Elasticsearch (index daté trips-YYYY.MM.DD)."""
    docs: List[Dict[str, Any]] = ti.xcom_pull(task_ids="TransformJson") or []
    if not docs:
        print("[PutElasticSearch] Nothing to index.")
        return 0

    es = Elasticsearch(ELASTIC_URL)
    index_name = f"{ES_INDEX_PREFIX}-{datetime.utcnow():%Y.%m.%d}"

    actions = ({"_index": index_name, "_source": d} for d in docs)
    helpers.bulk(es, actions, chunk_size=500)
    print(f"[PutElasticSearch] Indexed {len(docs)} docs to '{index_name}'")
    return len(docs)

def task_put_gcp(ti, **_) -> int:
    """
    Écrit un parquet partitionné dans EXPORT_DIR.
    Si ENABLE_GCS_UPLOAD=true & GCS_BUCKET défini & lib installée,
    uploade aussi vers GCS sous gs://{bucket}/{GCS_PREFIX}/year=.../month=.../day=.../hour=.../file.parquet
    """
    docs: List[Dict[str, Any]] = ti.xcom_pull(task_ids="TransformJson") or []
    if not docs:
        print("[PutGCP] Nothing to export/upload.")
        return 0

    now = datetime.utcnow()
    part_dir = EXPORT_DIR / f"year={now:%Y}/month={now:%m}/day={now:%d}/hour={now:%H}"
    part_dir.mkdir(parents=True, exist_ok=True)
    file_path = part_dir / f"part-{int(now.timestamp())}.parquet"

    df = pd.DataFrame(docs)
    df.to_parquet(file_path, index=False)
    print(f"[PutGCP] Exported {len(df)} rows to {file_path}")

    # Upload GCS optionnel
    if ENABLE_GCS_UPLOAD and GCS_BUCKET:
        gcs_key = f"{GCS_PREFIX}/year={now:%Y}/month={now:%m}/day={now:%d}/hour={now:%H}/{file_path.name}"
        uri = _gcs_upload_if_enabled(file_path, gcs_key)
        if uri:
            print(f"[PutGCP] Uploaded to {uri}")
        else:
            print("[PutGCP] GCS upload not performed (missing lib/creds or disabled).")

    return len(docs)

# =======================
#        DAG
# =======================
default_args = {
    "owner": "datastream",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="transform_json_v2",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/3 * * * *",  # toutes les 3 minutes
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=5),
    tags=["kafka", "elasticsearch", "gcp", "transform"],
) as dag:

    t1 = PythonOperator(
        task_id="ConsumKafka",
        python_callable=task_consume,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="TransformJson",
        python_callable=task_transform,
        provide_context=True,
    )

    t3_es = PythonOperator(
        task_id="PutElasticSearch",
        python_callable=task_put_es,
        provide_context=True,
    )

    t3_gcp = PythonOperator(
        task_id="PutGCP",
        python_callable=task_put_gcp,
        provide_context=True,
    )

    # Fan-out après transformation
    t1 >> t2 >> [t3_es, t3_gcp]