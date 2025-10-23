-- ====================================================================
-- Création de la table externe GCS
-- Auteur: Jiwon (iwannapaix@gmail.com)
-- Date: 2025-10-23
-- ====================================================================

CREATE OR REPLACE EXTERNAL TABLE 
  `votre-projet-gcp.taxi_streaming_project.trajets_streaming_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://nom-bucket-gcs/*.parquet'],
  description = "Données streaming taxi depuis NiFi/GCS"
);

SELECT COUNT(*) FROM `votre-projet-gcp.taxi_streaming_project.trajets_streaming_external`;