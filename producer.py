from kafka import KafkaProducer
import json

# Configuration du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Chargement du fichier JSON contenant tes trajets
with open("data/data_projet.json", "r") as f:
    data = json.load(f)

# Extraction de la liste des trajets
trajets = data.get("data", [])

# Publier chaque trajet dans le topic 'source' avec le format {"data": [...]}
for trajet in trajets:
    message = {"data": [trajet]}  # Important : envelopper dans "data"
    producer.send('source', value=message)

# S'assurer que tous les messages sont envoyés
producer.flush()

print(f"{len(trajets)} messages envoyés dans le topic 'source'")