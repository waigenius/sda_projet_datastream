# Datastream Starter (VS Code + Docker)

## Prérequis
- Docker Desktop + docker compose
- VS Code (recommandé: extension Docker)
- Réseau docker dédié: `docker network create datastream-net`

## 1) Génère un Cluster ID (KRaft)
```bash
docker run --rm confluentinc/cp-kafka:7.6.1 kafka-storage random-uuid
```
Copie le UUID et remplace toutes les occurrences de `REPLACE_ME_WITH_UUID` dans `docker-compose.yml`.

## 2) Démarre le cluster
```bash
docker compose up -d
```

## 3) Crée les topics
```bash
chmod +x kafka/create-topics.sh
./kafka/create-topics.sh
```

## 4) Envoie des événements de test
Installe `confluent-kafka` dans un venv local, ou lance le producteur depuis un conteneur Python:
```bash
# Option venv local
python3 -m venv .venv && source .venv/bin/activate
pip install confluent-kafka
python producer/producer.py

# Option conteneur (sans rien installer sur ta machine)
docker run --rm -it --network datastream-net -v "$PWD/producer":/app -w /app python:3.12   bash -lc "pip install confluent-kafka && python producer.py"
```

## 5) Airflow & Kibana
- Airflow UI: http://localhost:8080 (admin/admin)
- Kibana UI: http://localhost:5601

## Notes
- Ce dépôt est un **POC**: sécurité (TLS/SASL, xpack) non activée.
- Pour éteindre: `docker compose down` (les volumes conservent les données).
