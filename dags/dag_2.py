from __future__ import annotations
import json
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch, helpers
from google.cloud import storage

# --- Configuration ---
BOOTSTRAP_SERVERS = "kafka:9092"
IN_TOPIC = "result"
GROUP_ID = "dag2-transform-group"

ES_HOST = "http://elasticsearch:9200"
ES_INDEX = "rides-index"

GCS_BUCKET = "datastream-rides-bucket"

LOG_LEVEL = logging.INFO
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BATCH_SIZE = 100

# --- Task 1 : Consommer Kafka ---
def consume_from_kafka(**kwargs):
    from confluent_kafka import Consumer, KafkaException

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([IN_TOPIC])
    messages = []

    logger.info("Démarrage de la consommation Kafka DAG2...")
    try:
        while len(messages) < BATCH_SIZE:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.warning(f"Erreur Kafka: {msg.error()}")
                continue
            payload = json.loads(msg.value())
            messages.append(payload)
            logger.info(f"Message consommé: {payload}")
    except KafkaException as e:
        logger.error(f"Exception Kafka: {e}")
    finally:
        consumer.close()

    kwargs['ti'].xcom_push(key='kafka_messages', value=messages)
    logger.info(f"{len(messages)} messages consommés au total")

# --- Task 2 : Transformer JSON ---
def transform_json(**kwargs):
    ti = kwargs['ti']
    messages = ti.xcom_pull(task_ids='consume_kafka_dag2', key='kafka_messages') or []

    transformed_messages = []
    for msg in messages:
        for data in msg.get("data", []):
            flat = {
                "nomclient": data["properties-client"].get("nomclient"),
                "telephoneClient": data["properties-client"].get("telephoneClient"),
                "client_location": data["properties-client"].get("location"),
                "nomDriver": data["properties-driver"].get("nomDriver"),
                "telephoneDriver": data["properties-driver"].get("telephoneDriver"),
                "driver_location": data["properties-driver"].get("location"),
                "distance": data.get("Distance"),
                "confort": data.get("Confort"),
                "prix_travel": data.get("prix_travel"),
                "agent_timestamp": data.get("agent_timestamp"),
            }
            transformed_messages.append(flat)
            logger.info(f"Message transformé en plat: {flat}")

    ti.xcom_push(key='transformed_messages', value=transformed_messages)
    logger.info(f"Transformation terminée, total {len(transformed_messages)} messages")

# --- Task 3 : Indexer dans Elasticsearch ---
def put_elasticsearch(**kwargs):
    ti = kwargs['ti']
    messages = ti.xcom_pull(task_ids='transform_json', key='transformed_messages') or []

    es = Elasticsearch([ES_HOST])
    actions = [
        {
            "_index": ES_INDEX,
            "_source": msg
        }
        for msg in messages
    ]
    if actions:
        helpers.bulk(es, actions)
        logger.info(f"{len(messages)} documents indexés dans Elasticsearch ({ES_INDEX})")
    else:
        logger.info("Aucun document à indexer dans Elasticsearch")

# --- Task 4 : Envoyer dans GCS ---
def put_gcs(**kwargs):
    ti = kwargs['ti']
    messages = ti.xcom_pull(task_ids='transform_json', key='transformed_messages') or []

    if not messages:
        logger.info("Aucun message à envoyer vers GCS")
        return

    # ⚠️ Créer le client ici (et non au niveau global)
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    blob = bucket.blob(f"rides_{timestamp}.json")
    blob.upload_from_string(json.dumps(messages), content_type="application/json")
    logger.info(f"{len(messages)} messages envoyés dans GCS: {blob.name}")

# --- DAG ---
default_args = {
    "owner": "datastream",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    "dag2_transform_index_store",
    start_date=datetime(2025, 10, 25),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["datastream", "elasticsearch", "gcs"]
) as dag:

    consume_kafka_dag2 = PythonOperator(
        task_id="consume_kafka_dag2",
        python_callable=consume_from_kafka
    )

    transform_json_task = PythonOperator(
        task_id="transform_json",
        python_callable=transform_json
    )

    put_elasticsearch_task = PythonOperator(
        task_id="put_elasticsearch",
        python_callable=put_elasticsearch
    )

    put_gcs_task = PythonOperator(
        task_id="put_gcs",
        python_callable=put_gcs
    )

    consume_kafka_dag2 >> transform_json_task >> [put_elasticsearch_task, put_gcs_task]