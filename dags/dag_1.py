from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaConsumer, KafkaProducer
import json
from math import radians, cos, sin, asin, sqrt
import logging

# --- Logging de base pour voir tout dans le terminal ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# --- Fonction Haversine ---
def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371
    return c * r

# --- Consommer depuis Kafka ---
def consume_from_kafka(**kwargs):
    logging.info("Démarrage de la consommation Kafka...")
    consumer = KafkaConsumer(
        'source',
        bootstrap_servers='kafka:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='dag1-audit-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    messages = []
    try:
        for message in consumer:
            logging.info(f"Message reçu: {message.value}")
            messages.append(message.value)
            if len(messages) >= 100:  # batch limite
                break
    except Exception as e:
        logging.error(f"Erreur consommation Kafka: {e}")
    finally:
        consumer.close()
    logging.info(f"{len(messages)} messages consommés au total")
    return messages

# --- Calcul du coût ---
def compute_cost(**kwargs):
    ti = kwargs['ti']
    messages = ti.xcom_pull(task_ids='consume_task') or []

    all_trajets = []
    for msg in messages:
        if 'data' in msg:
            all_trajets.extend(msg['data'])
        else:
            all_trajets.append(msg)

    logging.info(f"Total trajets à calculer: {len(all_trajets)}")

    result_messages = []
    for trajet in all_trajets:
        client = trajet.get('properties-client', {})
        driver = trajet.get('properties-driver', {})

        try:
            distance = haversine(
                client['logitude'], client['latitude'],
                driver['logitude'], driver['latitude']
            )
        except KeyError:
            logging.warning("Clé manquante, distance=0")
            distance = 0

        prix_base = float(trajet.get('prix_base_per_km', 0))
        confort = trajet.get('confort', 'standard')
        coef_confort = {'low':1, 'standard':1.5, 'high':2}.get(confort, 1.5)
        prix_travel = round(prix_base * coef_confort * distance, 2)

        trajet_result = {
            "data": [{
                "properties-client": {
                    "nomclient": client.get('nomclient'),
                    "telephoneClient": client.get('telephoneClient'),
                    "location": f"{client.get('logitude')}, {client.get('latitude')}"
                },
                "properties-driver": {
                    "nomDriver": driver.get('nomDriver'),
                    "telephoneDriver": driver.get('telephoneDriver'),
                    "location": f"{driver.get('logitude')}, {driver.get('latitude')}"
                },
                "Distance": round(distance, 3),
                "prix_base_per_km": prix_base,
                "Confort": confort,
                "prix_travel": prix_travel
            }]
        }
        logging.info(f"Trajet calculé: {trajet_result}")
        result_messages.append(trajet_result)

    ti.xcom_push(key='result_messages', value=result_messages)
    logging.info("Tous les trajets calculés et poussés dans XCom.")

# --- Publier dans Kafka ---
def publish_to_kafka(**kwargs):
    ti = kwargs['ti']
    result_messages = ti.xcom_pull(task_ids='compute_task', key='result_messages') or []

    logging.info(f"Publication de {len(result_messages)} messages dans Kafka 'result'")
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    try:
        for msg in result_messages:
            logging.info(f"Envoi du message: {msg}")
            producer.send('result', value=msg)
        producer.flush()
    except Exception as e:
        logging.error(f"Erreur publication Kafka: {e}")

# --- Définition DAG ---
with DAG(
    'dag1_compute_cost',
    start_date=datetime(2025,10,23),
    catchup=False,
    default_args={'retries':2, 'retry_delay': timedelta(seconds=10)},
    tags=['audit','kafka']
) as dag:

    consume_task = PythonOperator(
        task_id='consume_task',
        python_callable=consume_from_kafka
    )

    compute_task = PythonOperator(
        task_id='compute_task',
        python_callable=compute_cost
    )

    publish_task = PythonOperator(
        task_id='publish_task',
        python_callable=publish_to_kafka
    )

    consume_task >> compute_task >> publish_task