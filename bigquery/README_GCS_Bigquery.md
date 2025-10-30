# Analyse BigQuery ML - Clustering K-Means

**Auteur:** Jiwon Yi  
**Date:** 30 octobre 2025  
**Rôle:** Analyse BigQuery et Machine Learning  
**Institution:** Université Paris 1 Panthéon-Sorbonne

---

## 🎯 Ma contribution

Configuration de Google Cloud Storage, création de pipeline de transformation automatique, et analyse K-Means avec BigQuery ML pour identifier les zones de taxi les plus rentables.

---

## 📊 Données reçues

**Source:** Fichiers JSON transformés par Cheikh (DAG2 Airflow)  
**Volume:** 6 000 trajets de taxi (New York)  
**Format:** JSON imbriqué avec coordonnées GPS, prix, confort

---

## ☁️ 1. Configuration Google Cloud Storage

### Bucket GCS créé
```
Nom: datastream-rides-bucket-*****
Région: EU (europe-west9)
Structure: Hive partitioning (year=/month=/day=/hour=/)
```

### Service Account
```
Service Account: taxi-streaming-*****@*****.gserviceaccount.com
Rôles: Storage Object Creator, BigQuery Data Editor
```

**Note:** Pour des raisons de sécurité, les informations complètes du projet GCP ne sont pas divulguées publiquement.

---

## 🔄 2. Pipeline de transformation automatique

### Script créé: `run_transform_hive.py`

**Fonction:** Transformer les JSON de Cheikh → Parquet → Upload GCS automatique

**Configuration GCS intégrée:**
```python
# Configuration (valeurs masquées pour sécurité)
GCS_BUCKET = os.getenv("GCS_BUCKET", "datastream-rides-bucket-*****")
PROJECT_ID = os.getenv("PROJECT_ID", "taxi-streaming-*****")
SERVICE_ACCOUNT_KEY = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Authentification
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_KEY
```

**Transformation:**
```python
# 1. Chargement JSON depuis exports/preview/
# 2. Aplatissement (flatten) des structures imbriquées
# 3. Ajout timestamp (agent_timestamp)
# 4. Export Parquet avec Hive partitioning
# 5. Upload automatique vers GCS
```

**Exécution:**
```bash
python run_transform_hive.py
```

**Résultat:**
```
✅ 6 000 trajets transformés
✅ Fichier créé: year=2025/month=10/day=30/hour=16/part-*.parquet
✅ Upload réussi vers GCS
```

---

## 📈 3. Analyse BigQuery ML

### A. External Table

```sql
-- Configuration External Table (chemin GCS masqué)
CREATE OR REPLACE EXTERNAL TABLE 
  `[PROJECT-ID].taxi_streaming_eu.taxi_rides_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://[BUCKET-NAME]/year=2025/month=10/day=30/hour=16/part-*.parquet']
);

-- Validation: 6 000 lignes ✓
```

### B. Modèle K-Means

**Dataset d'entraînement:** uber-split2.csv (132 418 points GPS New York)  
**Algorithme:** K-Means avec 8 clusters  
**Variables:** longitude, latitude

```sql
CREATE OR REPLACE MODEL 
  `[PROJECT-ID].taxi_streaming_eu.kmeans_model`
OPTIONS(
  model_type='kmeans',
  num_clusters=8,
  distance_type='euclidean'
) AS
SELECT lon as longitude, lat as latitude
FROM `[PROJECT-ID].taxi_streaming_eu.uber_data`;
```

**Justification K=8:**
- Représentation des 8 grandes zones de New York
- Équilibre entre granularité et interprétabilité
- Couverture optimale du territoire urbain

### C. Analyse CA par cluster

```sql
SELECT 
  CENTROID_ID,
  confort,
  COUNT(*) as nb_trajets,
  ROUND(SUM(prix_travel), 2) as CA,
  ROUND(AVG(prix_travel), 2) as prix_moyen,
  ROUND(AVG(distance), 2) as distance_moyenne
FROM ML.PREDICT(
  MODEL `[PROJECT-ID].taxi_streaming_eu.kmeans_model`,
  (
    SELECT 
      prix_travel, confort, distance,
      SAFE_CAST(TRIM(SPLIT(properties_client_location, ',')[OFFSET(0)]) AS FLOAT64) as longitude,
      SAFE_CAST(TRIM(SPLIT(properties_client_location, ',')[OFFSET(1)]) AS FLOAT64) as latitude
    FROM `[PROJECT-ID].taxi_streaming_eu.taxi_rides_external`
    WHERE confort IS NOT NULL
      AND properties_client_location IS NOT NULL
  )
)
GROUP BY CENTROID_ID, confort
ORDER BY CENTROID_ID, confort;
```

---

## 📊 4. Résultats

### Synthèse
- **Total trajets analysés:** 6 000
- **Clusters actifs:** 7 sur 8 (clusters 2,3,4,5,6,7,8)
- **Types de confort:** high, low, standard
- **CA total:** 150 516,56 €

### Distribution du CA par cluster

| Cluster | Trajets | CA Total (€) | % du CA | Distance moy. (km) |
|---------|---------|--------------|---------|-------------------|
| 4 | 1 438 | 53 008,44 | 35% | 17.12 |
| 8 | 1 182 | 41 344,16 | 27% | 15.27 |
| 5 | 960 | 28 978,86 | 19% | 13.85 |
| 6 | 900 | 27 099,06 | 18% | 13.67 |
| 2 | 522 | 20 825,66 | 14% | 17.47 |
| 7 | 592 | 17 506,84 | 12% | 13.64 |
| 3 | 406 | 11 739,52 | 8% | 13.30 |

### Détail par type de confort

**Segment "high" (confort premium):**
- CA: 93 692,86 € (62% du total)
- Prix moyen: 39-52 € par trajet
- Volume: 2 060 trajets (34%)
- **Conclusion:** Segment le plus rentable malgré volume modéré

**Segment "low" (confort économique):**
- CA: 44 837,48 € (30% du total)
- Prix moyen: 19-26 € par trajet
- Volume: 1 924 trajets (32%)
- **Conclusion:** Rapport volume/prix équilibré

**Segment "standard" (confort classique):**
- CA: 61 977,52 € (41% du total)
- Prix moyen: 27-37 € par trajet
- Volume: 2 016 trajets (34%)
- **Conclusion:** Segment intermédiaire stable

---

## 💡 5. Insights et recommandations

### Zone la plus rentable: Cluster 4
- **CA:** 53 008 € (35% du CA global)
- **Volume:** 1 438 trajets (24% du total)
- **Caractéristiques:** Distribution équilibrée des conforts, trajets longue distance (17 km)
- **Hypothèse:** Zone aéroportuaire ou trajets interurbains
- **Recommandation:** Augmenter la flotte de 30% dans cette zone

### Stratégie par segment
1. **High:** Cibler clientèle premium avec service VIP
2. **Standard:** Maintenir qualité/prix compétitif
3. **Low:** Optimiser les coûts opérationnels

### Zones à optimiser
- **Clusters 3 et 7:** CA faible (19 246 €), considérer réallocation de ressources
- **Cluster 1:** Inactif, analyser potentiel de développement

---

## 📁 Fichiers du projet

```
bigquery/
├── README.md                           # Ce document
├── bigquery_complete.sql               # Scripts SQL complets
├── run_transform_hive.py               # Pipeline de transformation
├── resultats_ca_clusters_final.csv     # Résultats K-Means (21 lignes)
└── bq-results-complet.csv              # Données complètes (6 000 lignes)
```

---

## 🔧 Installation et exécution

### Prérequis
```bash
Python 3.9+
pandas==2.0.0
pyarrow==13.0.0
google-cloud-storage==2.10.0
google-cloud-bigquery==3.11.0
```

### Variables d'environnement requises
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
export GCS_BUCKET="your-bucket-name"
export PROJECT_ID="your-project-id"
```

### Étapes d'exécution
```bash
# 1. Installation des dépendances
pip install -r requirements.txt

# 2. Configuration des credentials GCP
# (Configurer les variables d'environnement ci-dessus)

# 3. Transformation et upload
python run_transform_hive.py

# 4. Exécution des analyses SQL
# (Copier-coller les requêtes depuis bigquery_complete.sql dans BigQuery Console)
```

---

## 📊 Technologies utilisées

- **Google Cloud Storage:** Stockage des fichiers Parquet avec Hive partitioning
- **BigQuery:** Data warehouse et moteur SQL
- **BigQuery ML:** Modèle K-Means pour clustering géographique
- **Python:** pandas, pyarrow pour transformation des données
- **Parquet:** Format de fichier columnaire optimisé

---

## 👥 Équipe projet

- **Jiwon Yi:** Configuration GCS, BigQuery ML, analyse CA (ce travail)
- **Cheikh & Patricia:** DAGs Airflow, transformation JSON, Kafka/Elasticsearch

---

## 📚 Références

- [BigQuery ML Documentation](https://cloud.google.com/bigquery-ml/docs)
- [K-Means Clustering](https://cloud.google.com/bigquery-ml/docs/reference/standard-sql/bigqueryml-syntax-create-kmeans)
- [Hive Partitioning](https://cloud.google.com/bigquery/docs/hive-partitioned-loads-gcs)
- [Dataset Uber NYC](https://github.com/idiattara/Spark_DIATTARA/blob/main/uber-split2.csv)

---

## 📝 Notes de sécurité

⚠️ **Informations sensibles masquées dans ce README public:**
- Noms complets des buckets GCS
- Identifiants de projet GCP
- Adresses email complètes des Service Accounts
- Chemins complets des fichiers de credentials

Pour exécuter ce projet, vous devez configurer vos propres ressources GCP avec les permissions appropriées.

---

**Version:** 1.0  
**Dernière mise à jour:** 30 octobre 2025  
**Licence:** Projet académique - Tous droits réservés
