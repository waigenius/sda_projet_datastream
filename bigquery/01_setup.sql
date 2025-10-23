-- ====================================================================
-- Configuration initiale BigQuery
-- Auteur: Jiwon (iwannapaix@gmail.com)
-- Date: 2025-10-23
-- ====================================================================

CREATE SCHEMA IF NOT EXISTS `taxi-streaming-project.taxi_streaming_project`
OPTIONS(
  location="EU",
  description="Dataset BigQuery pour analyse taxi streaming"
);

SELECT 
  schema_name,
  location,
  creation_time
FROM `taxi-streaming-project.INFORMATION_SCHEMA.SCHEMATA`
WHERE schema_name = 'taxi_streaming_project';