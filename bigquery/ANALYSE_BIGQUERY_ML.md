# 📊 Analyse BigQuery ML - uber-split2.csv

**Projet**: Datastream Taxi Streaming  
**Auteur**: Jiwon  
**Date**: 2025-10-29  
**Dataset**: `taxi-streaming-project.taxi_streaming_eu`

---

## 🎯 Objectif

Utiliser BigQuery ML pour:
1. Créer un modèle **K-Means** avec 8 clusters basé sur uber-split2.csv
2. Analyser le **chiffre d'affaires** par cluster et type de confort
3. Identifier les zones géographiques les plus rentables

---

## 📁 Architecture des Données

### Sources de données
- **taxi_rides_external**: Table externe pointant vers GCS (format Parquet)
  - URI: `gs://datastream-rides-bucket/year=*/month=*/day=*/hour=*/*.parquet`
  - Partitionnement: année/mois/jour/heure
  - Records: 45 trajets

- **uber_data**: Données d'entraînement K-Means
  - Source: `uber-split2.csv` (132,418 lignes)
  - Colonnes: lat, lon, datetime, base
  - Zone géographique: New York City

### Pipeline de données
```
Kafka → Airflow DAG1 (calcul coût) → Kafka →
Airflow DAG2 (transformation) → GCS (Parquet) + Elasticsearch →
BigQuery (analyse ML)
```

---

## 🤖 Modèle K-Means

### Configuration
- **Algorithm**: K-Means avec initialisation K-Means++
- **Nombre de clusters**: 8
- **Features**: longitude, latitude (standardisées)
- **Dataset d'entraînement**: uber_data (132,418 points)

### Métriques de qualité
- **Davies-Bouldin Index**: 0.7009 ✅
  - Score < 1.0 = bon clustering
  - Clusters bien séparés
- **Distance quadratique moyenne**: 0.5093
- **Convergence**: 3 itérations

### Distribution des clusters

| Cluster ID | Nombre de points | Latitude | Longitude | Zone approximative |
|-----------|------------------|----------|-----------|-------------------|
| 1         | 1,778            | 40.7371  | -73.9933  | Manhattan Central |
| 2         | 6                | 41.0248  | -74.1104  | Bergen County, NJ |
| 3         | 1                | 40.7206  | -73.2258  | Nassau County     |
| 4         | 16               | 40.9431  | -73.7562  | Yonkers           |
| 5         | 129              | 40.6714  | -73.7838  | JFK Airport area  |
| 6         | 478              | 40.6860  | -73.9601  | Brooklyn          |
| 7         | 4                | 40.8134  | -74.4637  | Morristown, NJ    |
| 8         | 545              | 40.7865  | -73.9513  | Upper Manhattan   |

**Interprétation géographique:**
- Cluster 1 (Manhattan) = plus forte densité de pickups
- Clusters 5, 6, 8 = zones résidentielles et aéroports (bon volume)
- Clusters 2, 3, 7 = zones périphériques (faible activité)

---

## 💰 Analyse du Chiffre d'Affaires

### Données analysées
- **Période**: Octobre 2025
- **Total trajets**: 45
- **Types de confort**: High, Medium, standard

### Résultats par cluster et confort

| Cluster | Confort  | Nb Trajets | CA Total (€) | Prix Moyen (€) | Distance Moy (km) |
|---------|----------|-----------|--------------|----------------|-------------------|
| **3**   | **High** | **22**    | **6,567.60** | **298.53**     | **60.64**        |
| **3**   | Medium   | 21        | 3,161.40     | 150.54         | 61.01            |
| **3**   | standard | 2         | 2,488.99     | 1,244.49       | 622.30           |

### Synthèse globale

| Métrique                    | Valeur       |
|----------------------------|--------------|
| **Chiffre d'affaires total** | **12,218 €** |
| **Nombre de trajets**       | 45           |
| **Prix moyen par trajet**   | 271.51 €     |
| **Distance moyenne**        | 94.65 km     |

**Observation clé:** 🔍  
Tous les trajets sont concentrés dans le **Cluster 3** (Nassau County), ce qui suggère:
- Zone géographique spécifique du dataset actuel
- Possible limitation temporelle ou spatiale des données collectées

---

## 📈 Insights & Recommandations

### Points clés

1. **Confort "High" = le plus rentable**
   - 22 trajets (48.9% du total)
   - 6,567.60€ de CA (53.8% du total)
   - Prix moyen: 298.53€ (2x supérieur à Medium)

2. **Confort "Medium" = volume élevé**
   - 21 trajets (46.7% du total)
   - 3,161.40€ de CA (25.9% du total)
   - Bon compromis prix/volume

3. **Confort "standard" = anomalie**
   - Seulement 2 trajets
   - Distance moyenne très élevée: 622.30 km (!!)
   - Prix moyen: 1,244.49€ → trajets longue distance

### Distribution du CA par confort

```
High (53.8%)     ████████████████████████
Medium (25.9%)   ████████████
standard (20.4%) ██████████
```

### Recommandations stratégiques

#### Court terme
- ✅ **Augmenter la flotte "High"** dans le Cluster 3
- ✅ **Maintenir l'offre "Medium"** (volume important)
- ⚠️  **Analyser les trajets "standard"** (distance anormale)

#### Moyen terme
- 📍 **Élargir la couverture géographique** pour exploiter les autres clusters
- 💡 **Tarification dynamique** selon le cluster et le confort
- 📊 **Collecter plus de données** pour valider les tendances

#### Amélioration du modèle
- 🔄 **Réentraîner K-Means** avec les données de trajets réels
- 📈 **Augmenter le nombre de clusters** (10-12) pour plus de granularité
- 🧪 **Tester d'autres features**: heure de la journée, jour de la semaine

---

## 🔍 Analyse de la distribution géographique

### Concentration dans Cluster 3
Le fait que 100% des trajets soient dans le Cluster 3 peut indiquer:

**Hypothèses:**
1. Dataset de test limité géographiquement
2. Service de taxi concentré sur Nassau County
3. Données collectées sur une période/zone spécifique

**Action:** Vérifier les coordonnées dans `taxi_rides_external`:
```sql
SELECT 
  MIN(SAFE_CAST(REGEXP_EXTRACT(locationClient, r'^([^,]+)') AS FLOAT64)) as min_lon,
  MAX(SAFE_CAST(REGEXP_EXTRACT(locationClient, r'^([^,]+)') AS FLOAT64)) as max_lon,
  MIN(SAFE_CAST(REGEXP_EXTRACT(locationClient, r',\s*(.+)$') AS FLOAT64)) as min_lat,
  MAX(SAFE_CAST(REGEXP_EXTRACT(locationClient, r',\s*(.+)$') AS FLOAT64)) as max_lat
FROM `taxi-streaming-project.taxi_streaming_eu.taxi_rides_external`;
```

---

## 🛠️ Requêtes SQL Utilisées

### 1. Création du modèle K-Means
```sql
CREATE OR REPLACE MODEL `taxi-streaming-project.taxi_streaming_eu.kmeans_location_model`
OPTIONS(model_type='kmeans', num_clusters=8) AS
SELECT CAST(lon AS FLOAT64) as longitude, CAST(lat AS FLOAT64) as latitude
FROM `taxi-streaming-project.taxi_streaming_eu.uber_data`;
```

### 2. Analyse du chiffre d'affaires
```sql
WITH rides_with_coords AS (
  SELECT *, 
    SAFE_CAST(REGEXP_EXTRACT(locationClient, r'^([^,]+)') AS FLOAT64) AS client_lon,
    SAFE_CAST(REGEXP_EXTRACT(locationClient, r',\s*(.+)$') AS FLOAT64) AS client_lat
  FROM `taxi-streaming-project.taxi_streaming_eu.taxi_rides_external`
)
SELECT pred.CENTROID_ID, r.confort, COUNT(*) as nb_trajets,
  ROUND(SUM(r.prix_travel), 2) as ca_total
FROM rides_with_coords r
JOIN ML.PREDICT(MODEL `...kmeans_location_model`, (...)) pred
ON r.client_lon = pred.longitude AND r.client_lat = pred.latitude
GROUP BY pred.CENTROID_ID, r.confort
ORDER BY ca_total DESC;
```

---

## 📊 Visualisations Recommandées (Kibana/Looker)

1. **Carte géographique** des pickups avec couleur par cluster
2. **Histogramme** du CA par confort
3. **Scatter plot** longitude/latitude avec clusters colorés
4. **Time series** des trajets par heure de la journée
5. **Heatmap** distance vs prix par confort

---

## 🚀 Prochaines Étapes

- [ ] Collecter plus de données pour couvrir tous les 8 clusters
- [ ] Analyser les trajets "standard" (distance 622km anormale)
- [ ] Créer un dashboard Kibana avec les métriques clés
- [ ] Automatiser le réentraînement du modèle (mensuel)
- [ ] Implémenter une tarification dynamique basée sur les clusters

---

## 📚 Références

- **Modèle BigQuery ML**: `taxi-streaming-project.taxi_streaming_eu.kmeans_location_model`
- **Table de données**: `taxi-streaming-project.taxi_streaming_eu.taxi_rides_external`
- **GitHub**: [waigenius/sda_projet_datastream](https://github.com/waigenius/sda_projet_datastream)
- **Documentation**: [BigQuery ML K-Means](https://cloud.google.com/bigquery/docs/kmeans-tutorial)

---

**Dernière mise à jour**: 2025-10-29 18:00  
**Statut**: ✅ Analyse complète | 📊 Résultats validés
