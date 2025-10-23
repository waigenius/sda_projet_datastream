-- ====================================================================
-- Analyse du chiffre d'affaires par cluster et confort
-- Auteur: Jiwon (iwannapaix@gmail.com)
-- Date: 2025-10-23
-- ====================================================================

CREATE OR REPLACE TABLE 
  `taxi-streaming-project.taxi_streaming_project.trajets_avec_coordonnees` AS
SELECT 
  *,
  SAFE_CAST(TRIM(REGEXP_EXTRACT(locationClient, r'^([^,]+)')) AS FLOAT64) AS client_lon,
  SAFE_CAST(TRIM(REGEXP_EXTRACT(locationClient, r',\s*(.+)$')) AS FLOAT64) AS client_lat
FROM `taxi-streaming-project.taxi_streaming_project.trajets_streaming_external`
WHERE locationClient IS NOT NULL AND prix_travel IS NOT NULL;

CREATE OR REPLACE TABLE 
  `taxi-streaming-project.taxi_streaming_project.trajets_avec_clusters` AS
SELECT 
  t.*,
  p.CENTROID_ID as cluster_id
FROM `taxi-streaming-project.taxi_streaming_project.trajets_avec_coordonnees` t
LEFT JOIN (
  SELECT * FROM ML.PREDICT(
    MODEL `taxi-streaming-project.taxi_streaming_project.taxi_geocluster_model`,
    (SELECT nomClient, client_lon AS lon, client_lat AS lat
     FROM `taxi-streaming-project.taxi_streaming_project.trajets_avec_coordonnees`)
  )
) p ON t.nomClient = p.nomClient;

SELECT 
  cluster_id as CENTROID_ID,
  confort,
  ROUND(SUM(prix_travel), 2) as chiffre_affaire
FROM `taxi-streaming-project.taxi_streaming_project.trajets_avec_clusters`
WHERE cluster_id IS NOT NULL
GROUP BY cluster_id, confort
ORDER BY cluster_id;