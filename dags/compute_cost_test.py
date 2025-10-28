from __future__ import annotations
import json, os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Dict, List, Union

from airflow import DAG
from airflow.operators.python import PythonOperator

DATA_TEST_PATH = Path(os.getenv("DATA_TEST_PATH", "/opt/airflow/data/data_projet_vtest.json"))
EXPORT_DIR = Path(os.getenv("EXPORT_DIR", "/exports"))

def _ensure_list(payload: Union[Dict[str, Any], List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    return payload if isinstance(payload, list) else [payload]

def task_read_file(**_) -> List[Dict[str, Any]]:
    if not DATA_TEST_PATH.exists():
        raise FileNotFoundError(f"Fichier introuvable: {DATA_TEST_PATH}")
    with open(DATA_TEST_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    docs = _ensure_list(data)
    print(f"[ConsumFile] Read {len(docs)} item(s) from {DATA_TEST_PATH}")
    return docs  # XCom

def task_compute_fr(ti, **_) -> Dict[str, Any]:
    docs: List[Dict[str, Any]] = ti.xcom_pull(task_ids="ConsumKafkaFile") or []
    out_docs: List[Dict[str, Any]] = []

    for d in docs:
        # champs attendus (FR)
        distance = float(d.get("distance", 0.0))
        prix_base = float(d.get("prix_base_per_km", 2.0))
        prix_travel = round(prix_base * distance, 2)

        out_doc = dict(d)
        out_doc["prix_travel"] = prix_travel
        out_docs.append(out_doc)

    # Emballage exactement comme la capture: {"data": [ {...}, {...} ]}
    wrapped = {"data": out_docs}
    print("[ComputCostTravel] OUTPUT PREVIEW:\n" + json.dumps(wrapped, ensure_ascii=False, indent=2))
    return wrapped

def task_save_preview(ti, **_) -> str:
    wrapped: Dict[str, Any] = ti.xcom_pull(task_ids="ComputCostTravelFR") or {"data": []}
    ts = datetime.utcnow()
    out_dir = EXPORT_DIR / "preview"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"preview-{ts:%Y%m%dT%H%M%SZ}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(wrapped, f, ensure_ascii=False, indent=2)
    print(f"[PublishPreview] Wrote {out_path}")
    return str(out_path)

default_args = {"owner": "datastream", "depends_on_past": False, "retries": 0}

with DAG(
    dag_id="dag1_test_compute_cost",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # manuel
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=5),
    tags=["test", "local", "no-kafka"],
) as dag:

    t1 = PythonOperator(
        task_id="ConsumKafkaFile",
        python_callable=task_read_file,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="ComputCostTravelFR",
        python_callable=task_compute_fr,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id="PublishPreview",
        python_callable=task_save_preview,
        provide_context=True,
    )

    t1 >> t2 >> t3