# 📘 Projet Datastream

## 🧩 Objectif
Ce projet met en place une chaîne complète de traitement de données en temps réel : Kafka → Airflow → Elasticsearch → Kibana.
L’objectif est de simuler des trajets (`rides`), de calculer leur coût via Airflow, puis de les indexer dans Elasticsearch pour analyse et visualisation.

---

## 🚀 Architecture

```
Producer (Python)
    ↓
Kafka (rides.source / rides.result)
    ↓
Airflow DAG 1: compute_cost
    → calcule distance + coût
    → republie dans rides.result
    ↓
Airflow DAG 2: transform_json
    → aplatit + ajoute un horodatage + envoie dans Elasticsearch
    ↓
Elasticsearch → Kibana (visualisation)
```

---

## 🧱 Services Docker
| Service | Port | Description |
|----------|------|-------------|
| Kafka | - | 3 brokers KRaft |
| Schema Registry | 8081 | Gestion des schémas |
| Airflow Web UI | 8080 | Orchestration des DAGs |
| Elasticsearch | 9200 | Indexation et recherche |
| Kibana | 5601 | Interface graphique |
| PostgreSQL + Redis | internes | Backend Airflow |

---

## ⚙️ Prérequis
- Docker Desktop
- VS Code ou terminal
- Port 8080 et 5601 libres
- (Optionnel) `jq` pour formater le JSON

---

## 🏗️ Installation

```bash
git clone -b branche-cic https://github.com/waigenius/sda_projet_datastream.git
cd sda_projet_datastream/projet-ds
docker network create datastream-net
docker compose up -d
```

---

## 🔁 Pipeline de traitement

### 1️⃣ Créer les topics Kafka
```bash
chmod +x kafka/create-topics.sh
./kafka/create-topics.sh
```

### 2️⃣ Générer des trajets de test
```bash
docker run --rm -it --network datastream-net   -v "$PWD/producer":/app -w /app python:3.12   bash -lc "python -m pip install -q confluent-kafka && python producer.py"
```

### 3️⃣ Lancer les DAGs Airflow
```bash
docker exec -it airflow-web airflow dags trigger compute_cost
docker exec -it airflow-web airflow dags trigger transform_json
```

---

## 🔍 Vérification des données

### Kafka
```bash
docker exec -it kafka-1 kafka-console-consumer   --bootstrap-server kafka-1:9092   --topic rides.result --from-beginning --max-messages 3
```

### Elasticsearch
```bash
curl -s "http://localhost:9200/rides-000001/_count" | jq '.'
curl -s "http://localhost:9200/rides-000001/_search?size=3&pretty" | jq '.hits.hits[]._source'
```

### Kibana
- URL : [http://localhost:5601](http://localhost:5601)
- Crée un **Data View** : `rides-*` (champ `agent_timestamp`)
- Dans *Discover* : sélectionne “Last 7 days”
- Dans *Visualize Library* :
  - *Carte* : champ `pickup`
  - *Histogramme* : `Sum(cost_eur)` par `comfort`

---

## 🧠 Notes techniques
- Heap ES : `ES_JAVA_OPTS=-Xms768m -Xmx768m`
- Aucun `mem_limit` Docker pour éviter l’OOM Kill
- `transform_json` indexe par petits lots (`chunk_size=50`)
- Sécurité désactivée (POC local)

---

## 🧩 Dépannage
| Problème | Cause probable | Solution |
|-----------|----------------|-----------|
| ES s’arrête (exit 137) | OOM Kill | Augmente la heap ou ferme d’autres apps |
| Pas de données Kibana | Fenêtre de temps trop courte | “Last 30 days” ou relancer les DAGs |
| Airflow DAG absent | Erreur d’import | `airflow dags list-import-errors` |

---

## 👤 Auteur
Projet développé sur la **branche `branche-cic`**  
**Auteur :** [@ccheikh-ismael](https://github.com/ccheikh-ismael)  
Dépôt principal : [waigenius/sda_projet_datastream](https://github.com/waigenius/sda_projet_datastream)
