-- ====================================================================
-- Création de la table externe GCS
-- Auteur: Jiwon (iwannapaix@gmail.com)
-- Date: 2025-10-23
-- ====================================================================

CREATE OR REPLACE EXTERNAL TABLE 
  `taxi-streaming-project.taxi_streaming_project.trajets_streaming_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://taxi-streaming-data-bucket/*.parquet'],
  description = "Données streaming taxi depuis NiFi/GCS"
);

SELECT COUNT(*) FROM `taxi-streaming-project.taxi_streaming_project.trajets_streaming_external`;