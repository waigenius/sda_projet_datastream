-- ====================================================================
-- Modèle K-Means pour clustering géographique
-- Auteur: Jiwon (iwannapaix@gmail.com)
-- Date: 2025-10-23
-- ====================================================================

CREATE OR REPLACE TABLE 
  `taxi-streaming-project.taxi_streaming_project.uber_data_cleaned` AS
SELECT 
  CAST(latitude AS FLOAT64) AS latitude,
  CAST(longitude AS FLOAT64) AS longitude
FROM `taxi-streaming-project.taxi_streaming_project.uber_reference_data`
WHERE 
  latitude IS NOT NULL 
  AND longitude IS NOT NULL
  AND latitude BETWEEN -90 AND 90
  AND longitude BETWEEN -180 AND 180;

CREATE OR REPLACE MODEL 
  `taxi-streaming-project.taxi_streaming_project.taxi_geocluster_model`
OPTIONS(
  model_type = 'KMEANS',
  num_clusters = 8,
  standardize_features = TRUE
) AS
SELECT latitude AS lat, longitude AS lon
FROM `taxi-streaming-project.taxi_streaming_project.uber_data_cleaned`;

CREATE OR REPLACE TABLE 
  `taxi-streaming-project.taxi_streaming_project.cluster_centroids` AS
SELECT
  centroid_id as cluster_id,
  MAX(IF(feature = 'lat', numerical_value, NULL)) as latitude_centre,
  MAX(IF(feature = 'lon', numerical_value, NULL)) as longitude_centre
FROM ML.CENTROIDS(MODEL `taxi-streaming-project.taxi_streaming_project.taxi_geocluster_model`)
GROUP BY centroid_id;