import json
from datetime import datetime
from airflow import DAG
from kafka import KafkaProducer, KafkaConsumer
from airflow.operators.python_operator import PythonOperator

import calcul_distance

# --- Configuration des Topics et Connexion ---
KAFKA_CONN_ID = 'kfakasda.eastus.cloudapp.azure.com:9092'
SOURCE_TOPIC = "topic source"
RESULT_TOPIC = "topic result"

# Simuler le nombre de messages à traiter par lot pour cette exécution
MAX_MESSAGES_PER_RUN = 1 

# -----------------------------------------------------------------
# 1. Tâche : Consommer les Données Brutes
# -----------------------------------------------------------------
#kafka_servers = (KAFKA_CONN_ID)
def consum_kafka(**kwargs):
    """
    Consomme un lot de messages du topic source et les pousse vers XCom.
    """
    ti = kwargs['ti']
    messages_to_process = []

    try:
        consumer = KafkaConsumer(
            SOURCE_TOPIC,
            group_id="dag_1",
            bootstrap_servers=KAFKA_CONN_ID,
            auto_offset_reset='earliest',  # Pour lire depuis le début si aucun offset sauvegardé
            enable_auto_commit=True
        )

        print(f"Tentative de consommation de {MAX_MESSAGES_PER_RUN} message(s) du topic {SOURCE_TOPIC}...")

        for i, msg in enumerate(consumer):
            message_value = msg.value.decode('utf-8')
            print(f"Message lu : {message_value}")
            messages_to_process.append(message_value)

            if i + 1 >= MAX_MESSAGES_PER_RUN:
                break

        consumer.close()

    except Exception as e:
        print(f"Erreur lors de la consommation Kafka : {e}")

    if not messages_to_process:
        print("Aucun message à traiter. Arrêt de la tâche.")
        return []

    ti.xcom_push(key='consum_kafka_data', value=messages_to_process)
    print(f"Consommation terminée. {len(messages_to_process)} message(s) poussé(s) vers XCom.")
    return len(messages_to_process)

# -----------------------------------------------------------------
# 2. Tâche : Calculer le Coût du Trajet
# -----------------------------------------------------------------

def compute_cost_travel(**kwargs):
    """
    Récupère les messages bruts (via XCom), calcule le coût pour chaque message,
    et pousse les résultats dans XCom.
    """
    ti = kwargs['ti']
    # 1. Récupérer la liste des messages bruts de la tâche précédente
    raw_messages_list = ti.xcom_pull(task_ids='consum_kafka', key='consum_kafka_data')
    
    if not raw_messages_list:
        print("Aucun message brut trouvé pour le calcul.")
        return []

    result_cost_travel = []
    
    for raw_message_json in raw_messages_list:
        try:
            document = json.loads(raw_message_json)
            
            # Recuperation des champs pour le calcul du coût
            prix_base = document['prix_base_per_km']

            lon_client = document['properties-client']['logitude']
            lat_client = document['properties-client']['latitude']

            lon_driver = document['properties-driver']['logitude']
            lat_driver = document['properties-driver']['latitude']

            # Calcul de la distance
            distance = calcul_distance(lon_client, lat_client, lon_driver, lat_driver)
            
            # Calcul : Coût Total = (Prix de base * Distance)
            prix_travel = prix_base * distance

            # Création du champ 'location' au format "lon,lat"
            document["properties-client"]["location"] = f"{lon_client}, {lat_client}"
            document["properties-driver"]["location"] = f"{lon_driver}, {lat_driver}"

            # Ajout des nouveaux champs dans le document
            document["distance"] = round(distance, 3)
            document["prix_travel"] = round(prix_travel, 2)

            # Suppression des champs longitude et latitude
            del document["properties-client"]["latitude"]
            del document["properties-client"]["logitude"]
            del document["properties-driver"]["latitude"]
            del document["properties-driver"]["logitude"]

            # Préparation du document résultat
            result_document = {
                "properties-client": document["properties-client"],
                "distance": document["distance"],
                "properties-driver": document["properties-driver"],
                "prix_base_per_km": document["prix_base_per_km"],
                "confort": document["confort"],
                "prix_travel": document["prix_travel"]
            }
            
            result_cost_travel.append(json.dumps(result_document))
            
        except Exception as e:
            print(f"Erreur lors du calcul du coût pour un message: {e}")
            continue

    # Pousser la liste des messages traités vers XCom pour la production
    ti.xcom_push(key='result_cost_travel', value=result_cost_travel)
    print(f"Calcul terminé. {len(result_cost_travel)} message(s) traité(s).")
    return len(result_cost_travel)


# -----------------------------------------------------------------
# 3. Tâche : Produire le Résultat dans Kafka (Producteur)
# -----------------------------------------------------------------

def publish_kafka(**kwargs):

    """
    Récupère les messages traités (via XCom) et les produit dans le topic result.
    """
    ti = kwargs['ti']

    # 1️⃣ Récupération des messages à produire
    processed_messages_list = ti.xcom_pull(
        task_ids='compute_cost_travel',
        key='result_cost_travel'
    )

    if not processed_messages_list:
        print("Aucun message traité à produire. Arrêt de la tâche.")
        return 0

    try:
        # 2️⃣ Initialisation du producteur Kafka
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_CONN_ID,
            key_serializer=lambda k: k,  # les clés sont déjà encodées
            value_serializer=lambda v: v  # les valeurs aussi
        )

        # 3️⃣ Préparer et envoyer les messages
        for message_json in processed_messages_list:
            try:
                message_dict = json.loads(message_json)

                client_name = message_dict['properties-client']['nomclient']
                driver_name = message_dict['properties-driver']['nomDriver']
                key = f"{client_name}_{driver_name}".encode('utf-8')

                value = message_json.encode('utf-8')

                producer.send(RESULT_TOPIC, key=key, value=value)
                print(f"Message envoyé pour client {client_name} - chauffeur {driver_name}")

            except Exception as inner_e:
                print(f"Erreur sur un message : {inner_e}")
                continue

        # 4️⃣ Attendre la confirmation d'envoi de tous les messages
        producer.flush()

        print(f"✅ Production terminée : {len(processed_messages_list)} message(s) envoyé(s) au topic {RESULT_TOPIC}.")
        return len(processed_messages_list)

    except Exception as e:
        print(f"❌ Erreur critique lors de la production vers Kafka : {e}")
        raise

# -----------------------------------------------------------------
# DÉFINITION DU DAG
# -----------------------------------------------------------------

with DAG(
    dag_id='dag_1_cost_travel',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['cost_travel', 'distance_travel']
) as dag:
    
    consum_kafka_task= PythonOperator(
        task_id='consum_kafka',
        python_callable=consum_kafka,
    )
    
    compute_cost_travel_task= PythonOperator(
        task_id='compute_cost_travel',
        python_callable=compute_cost_travel,
    )
    
    publish_kafka_task = PythonOperator(
        task_id='publish_kafka',
        python_callable=publish_kafka,
    )
    
    # Définition de l'ordre d'exécution
    consum_kafka_task >> compute_cost_travel_task >> publish_kafka_task