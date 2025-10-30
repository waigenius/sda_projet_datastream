from __future__ import annotations
from pathlib import Path
import json
from typing import Any, Dict, List, Optional
import os
from datetime import datetime, timedelta

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
PRESENTATION_DIR = Path(os.getenv("PRESENTATION_DIR", "/exports/presentation"))

# ✅ Configuration GCS pour upload automatique
GCS_BUCKET = os.getenv("GCS_BUCKET", "datastream-rides-bucket").strip()  # ← Votre bucket
GCS_PREFIX = os.getenv("GCS_PREFIX", "").strip()  # Pas de préfixe (root du bucket)
ENABLE_GCS_UPLOAD = os.getenv("ENABLE_GCS_UPLOAD", "true").lower() == "true"  # ← Activé par défaut

# ✅ Chemin vers la clé Service Account (à adapter selon votre environnement)
SERVICE_ACCOUNT_KEY = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS", 
    "/opt/airflow/dags/taxi-streaming-project-ca97054b822a.json"
)

# =======================
#    HELPERS
# =======================
def flatten(d: Dict[str, Any], prefix: str = "", sep: str = "_") -> Dict[str, Any]:
    """Aplatit un dictionnaire imbriqué en structure plate."""
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
        # ✅ Configuration de l'authentification GCS
        if os.path.exists(SERVICE_ACCOUNT_KEY):
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_KEY
            print(f"[GCS] Utilisation du Service Account: {SERVICE_ACCOUNT_KEY}")
        else:
            print(f"[GCS] ATTENTION: Clé Service Account introuvable: {SERVICE_ACCOUNT_KEY}")
        
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET)
        blob = bucket.blob(gcs_uri_path)
        blob.upload_from_filename(str(local_file))
        
        uri = f"gs://{GCS_BUCKET}/{gcs_uri_path}"
        print(f"[GCS] ✅ Upload réussi: {uri}")
        return uri
        
    except Exception as e:
        print(f"[GCS] ❌ Erreur lors de l'upload: {e}")
        return None

# =======================
#     TASKS
# =======================
def task_consume(**_) -> List[Dict[str, Any]]:
    """Lit localement le fichier de sortie du DAG1 (prévisualisation) pour test."""
    # === Chemin du fichier local à lire ===
    TEST_FILE = Path("/exports/preview/preview-20251029T011356Z.json")  # <-- adapte ce nom à ton fichier
    if not TEST_FILE.exists():
        raise FileNotFoundError(f"Fichier de test introuvable : {TEST_FILE}")

    with open(TEST_FILE, "r", encoding="utf-8") as f:
        raw = json.load(f)

    docs: List[Dict[str, Any]] = []
    if isinstance(raw, dict) and "data" in raw:
        docs.extend(raw["data"])
    elif isinstance(raw, list):
        docs.extend(raw)
    else:
        docs.append(raw)

    print(f"[ConsumFile-TEST] Lecture de {len(docs)} documents depuis {TEST_FILE}")
    return docs

def task_transform(ti, **_) -> List[Dict[str, Any]]:
    """Transforme les données brutes en format plat avec timestamp."""
    raw_docs: List[Dict[str, Any]] = ti.xcom_pull(task_ids="ConsumKafka") or []
    if not raw_docs:
        print("[TransformJson] Aucune donnée à transformer.")
        return []

    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    out: List[Dict[str, Any]] = []
    
    for d in raw_docs:
        try:
            d = dict(d)  # copie
            d["agent_timestamp"] = ts
            flat = flatten(d)
            
            # Harmonisation minimale FR/EN des champs (optionnel)
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
            print(f"[TransformJson] Échec de transformation: {e}")

    print(f"[TransformJson] {len(out)} documents transformés")

    # ==========================================================
    # Création d'une version simplifiée (présentation attendue)
    # ==========================================================
    simplified_list = []
    for d in out:
        simp = {
            "nomclient": d.get("properties-client_nomclient") or d.get("nomclient", ""),
            "telephoneClient": d.get("properties-client_telephoneClient") or d.get("telephoneClient", ""),
            "locationClient": d.get("properties-client_location") or "",
            "distance": float(d.get("distance", 0)),
            "confort": d.get("confort", ""),
            "prix_travel": float(d.get("prix_travel", 0)),
            "nomDriver": d.get("properties-driver_nomDriver") or d.get("nomDriver", ""),
            "locationDriver": d.get("properties-driver_location") or "",
            "telephoneDriver": d.get("properties-driver_telephoneDriver") or d.get("telephoneDriver", ""),
            "agent_timestamp": d.get("agent_timestamp", ""),
        }
        simplified_list.append(simp)

    # ==========================================================
    # Écriture JSON locale (format présentation)
    # ==========================================================
    try:
        json_dir = Path("/exports/json")
        json_dir.mkdir(parents=True, exist_ok=True)
        filename = f"transform_presentation_{datetime.utcnow():%Y%m%dT%H%M%S}.json"
        file_path = json_dir / filename

        to_dump = simplified_list[0] if len(simplified_list) == 1 else simplified_list

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(to_dump, f, ensure_ascii=False, indent=3)

        print(f"[TransformJson] JSON de présentation sauvegardé: {file_path}")
    except Exception as e:
        print(f"[TransformJson] ATTENTION: échec d'écriture du JSON de présentation: {e}")

    # XCom push de la version présentation
    ti.xcom_push(key="pretty_docs", value=simplified_list)

    return out

def task_put_es(ti, **_) -> int:
    """Indexe dans Elasticsearch (index daté trips-YYYY.MM.DD)."""
    docs: List[Dict[str, Any]] = ti.xcom_pull(task_ids="TransformJson") or []
    if not docs:
        print("[PutElasticSearch] Aucun document à indexer.")
        return 0

    es = Elasticsearch(ELASTIC_URL)
    index_name = f"{ES_INDEX_PREFIX}-{datetime.utcnow():%Y.%m.%d}"

    actions = ({"_index": index_name, "_source": d} for d in docs)
    helpers.bulk(es, actions, chunk_size=500)
    print(f"[PutElasticSearch] {len(docs)} documents indexés dans '{index_name}'")
    return len(docs)

def task_put_gcp(ti, **_) -> int:
    """
    Exporte les données en Parquet avec Hive partitioning et upload vers GCS.
    Structure: year=YYYY/month=MM/day=DD/hour=HH/part-{timestamp}.parquet
    """
    docs: List[Dict[str, Any]] = ti.xcom_pull(task_ids="TransformJson") or []
    if not docs:
        print("[PutGCP] Aucune donnée à exporter/uploader.")
        return 0

    now = datetime.utcnow()
    
    # ✅ Création du répertoire avec Hive partitioning
    part_dir = EXPORT_DIR / f"year={now:%Y}/month={now:%m}/day={now:%d}/hour={now:%H}"
    part_dir.mkdir(parents=True, exist_ok=True)
    file_path = part_dir / f"part-{int(now.timestamp())}.parquet"

    # ✅ Export en Parquet
    df = pd.DataFrame(docs)
    df.to_parquet(file_path, index=False)
    print(f"[PutGCP] {len(df)} lignes exportées vers {file_path}")

    # ✅ Upload GCS automatique
    if ENABLE_GCS_UPLOAD and GCS_BUCKET:
        # Construction du chemin GCS avec Hive partitioning
        if GCS_PREFIX:
            gcs_key = f"{GCS_PREFIX}/year={now:%Y}/month={now:%m}/day={now:%d}/hour={now:%H}/{file_path.name}"
        else:
            # Pas de préfixe → directement à la racine du bucket
            gcs_key = f"year={now:%Y}/month={now:%m}/day={now:%d}/hour={now:%H}/{file_path.name}"
        
        uri = _gcs_upload_if_enabled(file_path, gcs_key)
        if uri:
            print(f"[PutGCP] ✅ Upload GCS réussi: {uri}")
        else:
            print("[PutGCP] ⚠️  Upload GCS non effectué (lib/credentials manquants ou désactivé)")
    else:
        print("[PutGCP] Upload GCS désactivé (ENABLE_GCS_UPLOAD=false ou GCS_BUCKET vide)")

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
    dag_id="transform_json_test",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/3 * * * *",  # Toutes les 3 minutes
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
