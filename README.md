# 🎯 BigQuery ML - Guide d'utilisation (Jiwon)

## 📋 Vue d'ensemble

Ce guide explique comment:
1. Uploader les fichiers Parquet vers GCS
2. Créer une table externe BigQuery
3. Entraîner un modèle K-Means (8 clusters)
4. Analyser le chiffre d'affaires par cluster et type de confort

---

## 🔑 Prérequis

- ✅ Projet GCP: `taxi-streaming-project`
- ✅ Service Account: `bigquery-uploader@taxi-streaming-project.iam.gserviceaccount.com`


---

## 📦 Étape 1: Installation des dépendances

```bash
# Dans le terminal VSCode
pip install google-cloud-storage google-cloud-bigquery
```

---

## ☁️ Étape 2: Upload des fichiers Parquet vers GCS

### 2.1 Copier le script dans votre projet

```bash
# Copier upload_to_gcs.py dans votre dossier scripts/
cp upload_to_gcs.py scripts/
```

### 2.2 Vérifier la configuration

Ouvrez `scripts/upload_to_gcs.py` et vérifiez:

```python
PROJECT_ID = "taxi-streaming-project"  # ✅ OK
BUCKET_NAME = "datastream-rides-bucket"  # Nom de votre bucket
KEY_FILE = "taxi-streaming-project-ca97054b822a.json"  # ✅ OK
```

### 2.3 Exécuter l'upload

```bash
python scripts/upload_to_gcs.py
```

**Sortie attendue:**
```
============================================================
📤 Upload Parquet → GCS
============================================================
📦 Création du bucket 'datastream-rides-bucket'...
✅ Bucket créé: gs://datastream-rides-bucket/

🚀 Upload de 42 fichiers parquet vers gs://datastream-rides-bucket/

✅ year=2025/month=10/day=29/hour=00/part-1761696201.parquet
✅ year=2025/month=10/day=29/hour=00/part-1761696383.parquet
...

✨ Upload terminé: 42/42 fichiers uploadés avec succès!
📍 URI: gs://datastream-rides-bucket/year=*/month=*/day=*/hour=*/*.parquet
```

---

## 📊 Étape 3: Configuration BigQuery

### 3.1 Uploader uber-split2.csv

**Option A: Via l'interface BigQuery Console**
1. Allez sur https://console.cloud.google.com/bigquery
2. Sélectionnez le projet `taxi-streaming-project`
3. Créez le dataset `datastream_dataset` (si nécessaire)
4. Cliquez sur "Créer une table"
5. Source: Upload → Sélectionnez `data/uber-split2.csv`
6. Destination: 
   - Dataset: `datastream_dataset`
   - Table: `uber_data`
7. Schéma:
   - `lat`: FLOAT
   - `lon`: FLOAT
   - `datetime`: STRING
   - `base`: STRING

**Option B: Via la ligne de commande**

```bash
bq load \
  --source_format=CSV \
  --skip_leading_rows=1 \
  --project_id=taxi-streaming-project \
  datastream_dataset.uber_data \
  data/uber-split2.csv \
  lat:FLOAT,lon:FLOAT,datetime:STRING,base:STRING
```

### 3.2 Exécuter le script SQL

1. Ouvrez BigQuery Console: https://console.cloud.google.com/bigquery
2. Ouvrez le fichier `bigquery_ml_setup.sql`
3. Exécutez chaque section une par une (commentée avec `Étape X`)

**OU** exécutez tout le script:

```bash
bq query --project_id=taxi-streaming-project --use_legacy_sql=false < bigquery_ml_setup.sql
```

---

## 📈 Étape 4: Résultats attendus

### 4.1 Vérification des données

```sql
-- Nombre total de trajets
SELECT COUNT(*) FROM `taxi-streaming-project.datastream_dataset.rides_external`;
-- Résultat attendu: ~40-50 trajets
```

### 4.2 Visualisation des clusters

```sql
-- 8 clusters créés avec leurs centroïdes
SELECT CENTROID_ID, COUNT(*) as nb_points
FROM ML.PREDICT(...)
GROUP BY CENTROID_ID;
```

### 4.3 Chiffre d'affaires par cluster

```sql
-- CA total par cluster et type de confort
SELECT 
  cluster_id,
  type_confort,
  chiffre_affaire_total,
  nb_trajets
FROM (...)
ORDER BY chiffre_affaire_total DESC;
```

**Exemple de sortie:**

| cluster_id | type_confort | nb_trajets | chiffre_affaire_total | prix_moyen |
|------------|--------------|------------|-----------------------|------------|
| 3          | high         | 15         | 4523.50               | 301.57     |
| 1          | standard     | 22         | 3890.75               | 176.85     |
| 5          | low          | 8          | 1200.00               | 150.00     |

---

## 🐛 Dépannage

### Erreur: "Bucket n'existe pas"
```bash
# Créer le bucket manuellement
gsutil mb -p taxi-streaming-project gs://datastream-rides-bucket/
```

### Erreur: "Permission denied"
```bash
# Vérifier les permissions du service account
gcloud projects get-iam-policy taxi-streaming-project \
  --flatten="bindings[].members" \
  --filter="bindings.members:bigquery-uploader@*"
```

### Erreur: "Table not found"
```bash
# Vérifier que uber_data existe
bq show taxi-streaming-project:datastream_dataset.uber_data
```

---

## ✅ Checklist finale

- [ ] Fichiers Parquet uploadés vers GCS
- [ ] Bucket `datastream-rides-bucket` créé
- [ ] Dataset BigQuery `datastream_dataset` créé
- [ ] Table externe `rides_external` créée et accessible
- [ ] Table `uber_data` chargée avec CSV
- [ ] Modèle K-Means `kmeans_location_model` entraîné
- [ ] Requête de CA par cluster exécutée avec succès

---

**Auteur**: Jiwon  
**Date**: 2025-10-29  
**Projet**: Datastream Taxi Streaming Project
