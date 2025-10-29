-- ================================================================
-- BigQuery ML - Projet Datastream Taxi
-- Auteur: Jiwon
-- Date: 2025-10-29
-- ================================================================

-- ÉTAPE 1: Créer le dataset
-- ================================================================
CREATE SCHEMA IF NOT EXISTS `taxi-streaming-project.taxi_streaming_eu`
OPTIONS (
  location = 'europe-west1',
  description = 'Dataset pour les données de trajets taxi'
);

-- ÉTAPE 2: Créer la table externe (Parquet depuis GCS)
-- ================================================================
-- Note: Assurez-vous que les fichiers Parquet sont uploadés vers GCS
-- URI: gs://datastream-rides-bucket/year=*/month=*/day=*/hour=*/*.parquet

CREATE OR REPLACE EXTERNAL TABLE `taxi-streaming-project.taxi_streaming_eu.taxi_rides_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://datastream-rides-bucket/year=*/month=*/day=*/hour=*/*.parquet'],
  hive_partition_uri_prefix = 'gs://datastream-rides-bucket/',
  require_hive_partition_filter = false
);

-- Vérification des données
SELECT 
  COUNT(*) as total_rides,
  COUNT(DISTINCT confort) as nb_confort_types,
  ARRAY_AGG(DISTINCT confort IGNORE NULLS) as types_confort,
  MIN(prix_travel) as prix_min,
  MAX(prix_travel) as prix_max,
  ROUND(AVG(prix_travel), 2) as prix_moyen
FROM `taxi-streaming-project.taxi_streaming_eu.taxi_rides_external`;

-- ÉTAPE 3: Charger les données Uber CSV
-- ================================================================
-- Option A: Via la console BigQuery UI
-- Option B: Via bq command line:
-- bq load --source_format=CSV --skip_leading_rows=1 \
--   --project_id=taxi-streaming-project \
--   taxi_streaming_eu.uber_data \
--   data/uber-split2.csv \
--   lat:FLOAT,lon:FLOAT,datetime:STRING,base:STRING

CREATE OR REPLACE TABLE `taxi-streaming-project.taxi_streaming_eu.uber_data`
(
  lat FLOAT64,
  lon FLOAT64,
  datetime STRING,
  base STRING
);

-- Vérification uber_data
SELECT COUNT(*) as total_points FROM `taxi-streaming-project.taxi_streaming_eu.uber_data`;

-- ÉTAPE 4: Créer le modèle K-Means (8 clusters)
-- ================================================================
CREATE OR REPLACE MODEL `taxi-streaming-project.taxi_streaming_eu.kmeans_location_model`
OPTIONS(
  model_type='kmeans',
  num_clusters=8,
  standardize_features=TRUE,
  kmeans_init_method='KMEANS_PLUS_PLUS',
  max_iterations=20
) AS
SELECT
  CAST(lon AS FLOAT64) as longitude,
  CAST(lat AS FLOAT64) as latitude
FROM `taxi-streaming-project.taxi_streaming_eu.uber_data`
WHERE lon IS NOT NULL AND lat IS NOT NULL;

-- ÉTAPE 5: Évaluer le modèle
-- ================================================================
-- Métriques de qualité du clustering
SELECT *
FROM ML.EVALUATE(MODEL `taxi-streaming-project.taxi_streaming_eu.kmeans_location_model`);

-- Visualiser les centroïdes des 8 clusters
SELECT 
  CENTROID_ID,
  COUNT(*) as nb_points,
  ROUND(AVG(longitude), 4) as centroid_lon,
  ROUND(AVG(latitude), 4) as centroid_lat
FROM ML.PREDICT(
  MODEL `taxi-streaming-project.taxi_streaming_eu.kmeans_location_model`,
  (SELECT 
    CAST(lon AS FLOAT64) as longitude,
    CAST(lat AS FLOAT64) as latitude
   FROM `taxi-streaming-project.taxi_streaming_eu.uber_data`)
)
GROUP BY CENTROID_ID
ORDER BY CENTROID_ID;

-- ÉTAPE 6: Analyse du chiffre d'affaires par cluster et confort
-- ================================================================
-- Assigner chaque trajet à un cluster et calculer les revenus
WITH rides_with_coords AS (
  SELECT 
    *,
    SAFE_CAST(REGEXP_EXTRACT(locationClient, r'^([^,]+)') AS FLOAT64) AS client_lon,
    SAFE_CAST(REGEXP_EXTRACT(locationClient, r',\s*(.+)$') AS FLOAT64) AS client_lat
  FROM `taxi-streaming-project.taxi_streaming_eu.taxi_rides_external`
  WHERE locationClient IS NOT NULL
)

SELECT 
  pred.CENTROID_ID as cluster_id,
  r.confort as type_confort,
  COUNT(*) as nb_trajets,
  ROUND(SUM(r.prix_travel), 2) as chiffre_affaire_total,
  ROUND(AVG(r.prix_travel), 2) as prix_moyen,
  ROUND(AVG(r.distance), 2) as distance_moyenne_km
FROM rides_with_coords r
JOIN ML.PREDICT(
  MODEL `taxi-streaming-project.taxi_streaming_eu.kmeans_location_model`,
  (SELECT DISTINCT client_lon as longitude, client_lat as latitude 
   FROM rides_with_coords WHERE client_lon IS NOT NULL AND client_lat IS NOT NULL)
) pred
ON r.client_lon = pred.longitude AND r.client_lat = pred.latitude
WHERE r.confort IS NOT NULL AND r.prix_travel IS NOT NULL
GROUP BY cluster_id, type_confort
ORDER BY chiffre_affaire_total DESC;

-- ÉTAPE 7: Analyse globale par cluster (tous conforts confondus)
-- ================================================================
WITH rides_with_coords AS (
  SELECT 
    *,
    SAFE_CAST(REGEXP_EXTRACT(locationClient, r'^([^,]+)') AS FLOAT64) AS client_lon,
    SAFE_CAST(REGEXP_EXTRACT(locationClient, r',\s*(.+)$') AS FLOAT64) AS client_lat
  FROM `taxi-streaming-project.taxi_streaming_eu.taxi_rides_external`
  WHERE locationClient IS NOT NULL
)

SELECT 
  pred.CENTROID_ID as cluster_id,
  COUNT(*) as total_trajets,
  ROUND(SUM(r.prix_travel), 2) as ca_total,
  ROUND(AVG(r.prix_travel), 2) as prix_moyen,
  ROUND(AVG(r.distance), 2) as distance_moyenne_km,
  ROUND(SUM(r.prix_travel) * 100.0 / SUM(SUM(r.prix_travel)) OVER(), 2) as pct_ca_total
FROM rides_with_coords r
JOIN ML.PREDICT(
  MODEL `taxi-streaming-project.taxi_streaming_eu.kmeans_location_model`,
  (SELECT DISTINCT client_lon as longitude, client_lat as latitude 
   FROM rides_with_coords WHERE client_lon IS NOT NULL AND client_lat IS NOT NULL)
) pred
ON r.client_lon = pred.longitude AND r.client_lat = pred.latitude
WHERE r.prix_travel IS NOT NULL
GROUP BY cluster_id
ORDER BY ca_total DESC;

-- ÉTAPE 8: Top 3 clusters par type de confort
-- ================================================================
WITH rides_with_coords AS (
  SELECT 
    *,
    SAFE_CAST(REGEXP_EXTRACT(locationClient, r'^([^,]+)') AS FLOAT64) AS client_lon,
    SAFE_CAST(REGEXP_EXTRACT(locationClient, r',\s*(.+)$') AS FLOAT64) AS client_lat
  FROM `taxi-streaming-project.taxi_streaming_eu.taxi_rides_external`
  WHERE locationClient IS NOT NULL
),

ranked_clusters AS (
  SELECT 
    pred.CENTROID_ID,
    r.confort,
    SUM(r.prix_travel) as ca_total,
    COUNT(*) as nb_trajets,
    ROW_NUMBER() OVER (PARTITION BY r.confort ORDER BY SUM(r.prix_travel) DESC) as rang
  FROM rides_with_coords r
  JOIN ML.PREDICT(
    MODEL `taxi-streaming-project.taxi_streaming_eu.kmeans_location_model`,
    (SELECT DISTINCT client_lon as longitude, client_lat as latitude 
     FROM rides_with_coords WHERE client_lon IS NOT NULL AND client_lat IS NOT NULL)
  ) pred
  ON r.client_lon = pred.longitude AND r.client_lat = pred.latitude
  WHERE r.confort IS NOT NULL AND r.prix_travel IS NOT NULL
  GROUP BY pred.CENTROID_ID, r.confort
)

SELECT 
  confort,
  CENTROID_ID as cluster_id,
  nb_trajets,
  ROUND(ca_total, 2) as chiffre_affaire,
  rang
FROM ranked_clusters
WHERE rang <= 3
ORDER BY confort, rang;

-- ================================================================
-- FIN DU SCRIPT
-- ================================================================
