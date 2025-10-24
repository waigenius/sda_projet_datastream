# Documentation BigQuery - Projet Taxi Streaming

## Vue d'ensemble

Ce document décrit l'implémentation BigQuery pour le projet de streaming de données de taxi, incluant le Data Warehouse, le Machine Learning avec K-Means, et l'analyse des revenus.

## Architecture

- **Google Cloud Storage (GCS)** : Stockage des fichiers Parquet
- **BigQuery External Table** : Lecture des données depuis GCS
- **BigQuery ML** : Modèle K-Means avec 8 clusters
- **Analyse** : Calcul du chiffre d'affaires par cluster et type de confort

## Fichiers SQL

### 01_setup.sql
Création du dataset BigQuery dans la région europe-west9 (Paris).

### 02_external_table.sql
Création d'une table externe pointant vers le bucket GCS `taxi-streaming-data-bucket`.

### 03_kmeans_model.sql
Création d'un modèle K-Means avec 8 clusters basé sur les coordonnées géographiques (longitude, latitude) des données Uber de référence.

### 04_revenue_analysis.sql
Requête d'analyse calculant le chiffre d'affaires par cluster et type de confort (High, Medium, Standard).

## Configuration GCP

**Projet :** `taxi-streaming-project`  
**Région :** `europe-west9` (Paris)  
**Dataset :** `taxi_dataset`  
**Bucket :** `taxi-streaming-data-bucket`

## Modèle K-Means

- **Nombre de clusters :** 8
- **Variables :** longitude, latitude
- **Distance :** Euclidienne
- **Données d'entraînement :** uber-split2.csv

## Utilisation

### 1. Exécuter les scripts SQL dans l'ordre
```sql
-- 1. Créer le dataset
source sql/01_setup.sql

-- 2. Créer la table externe
source sql/02_external_table.sql

-- 3. Créer le modèle K-Means
source sql/03_kmeans_model.sql

-- 4. Analyser les revenus
source sql/04_revenue_analysis.sql
```

### 2. Scripts Python

**Génération de données de test :**
```bash
python scripts/generate_test_data.py
```

**Upload vers GCS :**
```bash
# Définir les credentials
$env:GOOGLE_APPLICATION_CREDENTIALS="path\to\key.json"

# Upload
python scripts/upload_to_gcs.py
```

## Résultats

Le modèle permet de :
- Segmenter géographiquement les courses de taxi en 8 zones
- Analyser le chiffre d'affaires par zone et niveau de confort
- Identifier les zones les plus rentables

## Équipe BigQuery

- Jiwon
- Bintou

## Prochaines étapes

- Intégration avec le pipeline NiFi (équipe Airflow)
- Visualisation dans Kibana
- Analyse en temps réel des données streaming
