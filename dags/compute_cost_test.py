from __future__ import annotations
import json, os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Dict, List, Union

from airflow import DAG
from airflow.operators.python import PythonOperator

import math

def _unwrap_payload(obj):
    # accepte {"data":[...]} OU une liste OU un objet
    if isinstance(obj, dict) and "data" in obj and isinstance(obj["data"], list):
        return obj["data"]
    return obj if isinstance(obj, list) else [obj]

def _parse_loc_string(s: str | None) -> tuple[float | None, float | None]:
    """Accepte 'lon, lat' et renvoie (lat, lon)."""
    if not s or "," not in s:
        return None, None
    try:
        lon_s, lat_s = s.split(",", 1)
        lon = float(lon_s.strip())
        lat = float(lat_s.strip())
        return lat, lon
    except Exception:
        return None, None

def _get_coord(d: dict) -> tuple[float | None, float | None]:
    """
    Récupère (lat, lon) en tolérant :
    - 'logitude' (typo) ou 'longitude'
    - 'lat'/'lon'
    - 'location' sous forme 'lon, lat'
    """
    lat = d.get("latitude") or d.get("lat")
    lon = d.get("longitude") or d.get("logitude") or d.get("lon")
    if lat is not None and lon is not None:
        try:
            return float(lat), float(lon)
        except Exception:
            pass
    # fallback : 'location': "lon, lat"
    if "location" in d:
        return _parse_loc_string(d.get("location"))
    return None, None

def _to_loc_string(lat: float | None, lon: float | None) -> str:
    if lat is None or lon is None:
        return ""
    # format attendu "lon, lat"
    return f"{lon:.4f}, {lat:.4f}"

def _haversine_km(lat1, lon1, lat2, lon2) -> float:
    """Distance en km entre deux points (lat/lon en degrés)."""
    R = 6371.0
    from math import radians, sin, cos, asin, sqrt
    φ1, λ1, φ2, λ2 = map(radians, [lat1, lon1, lat2, lon2])
    dφ, dλ = (φ2-φ1), (λ2-λ1)
    a = sin(dφ/2)**2 + cos(φ1)*cos(φ2)*sin(dλ/2)**2
    return R * 2 * asin(sqrt(a))

DATA_TEST_PATH = Path(os.getenv("DATA_TEST_PATH", "/opt/airflow/data/data_projet_vtest.json"))
EXPORT_DIR = Path(os.getenv("EXPORT_DIR", "/exports"))

def _ensure_list(payload: Union[Dict[str, Any], List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    return payload if isinstance(payload, list) else [payload]

def task_read_file(**_) -> List[Dict[str, Any]]:
    if not DATA_TEST_PATH.exists():
        raise FileNotFoundError(f"Fichier introuvable: {DATA_TEST_PATH}")
    with open(DATA_TEST_PATH, "r", encoding="utf-8") as f:
        raw = json.load(f)
    docs = _unwrap_payload(raw)  # <-- accepte {"data":[...]}
    print(f"[ConsumFile] Read {len(docs)} item(s) from {DATA_TEST_PATH}")
    return docs

def task_compute_fr(ti, **_) -> Dict[str, Any]:
    docs: List[Dict[str, Any]] = ti.xcom_pull(task_ids="ConsumKafkaFile") or []
    out_docs: List[Dict[str, Any]] = []

    for d in docs:
        confort = d.get("confort", "standard")
        prix_base = float(d.get("prix_base_per_km", 2.0))

        client = d.get("properties-client", {}) or {}
        driver = d.get("properties-driver", {}) or {}

        # Coordonnées (tolérance : longitude/logitude + location "lon, lat")
        c_lat, c_lon = _get_coord(client)
        d_lat, d_lon = _get_coord(driver)

        # Distance : on priorise 'distance', puis 'distance_km', sinon calcul Haversine
        distance_val = d.get("distance", d.get("distance_km"))
        try:
            distance = float(distance_val) if distance_val is not None else (
                _haversine_km(c_lat, c_lon, d_lat, d_lon) if None not in (c_lat, c_lon, d_lat, d_lon) else 0.0
            )
        except Exception:
            distance = 0.0

        prix_travel = round(prix_base * distance, 2)

        # Timestamp : conserver si présent, sinon générer maintenant (UTC)
        ts = d.get("agent_timestamp")
        if not ts:
            ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        out_doc = {
            "properties-client": {
                "nomclient": client.get("nomclient", ""),
                "telephoneClient": client.get("telephoneClient", ""),
                # format "lon, lat" attendu par ta maquette
                "location": _to_loc_string(c_lat, c_lon),
            },
            "distance": distance,
            "properties-driver": {
                "nomDriver": driver.get("nomDriver", ""),
                "location": _to_loc_string(d_lat, d_lon),
                "telephoneDriver": driver.get("telephoneDriver", ""),
            },
            "prix_base_per_km": prix_base,
            "confort": confort,
            "prix_travel": prix_travel,
            "agent_timestamp": ts,  # ✅ désormais dans le preview
        }
        out_docs.append(out_doc)

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