FROM apache/airflow:2.9.3

USER airflow

# Installer les librairies Python nécessaires
RUN pip install --no-cache-dir kafka-python elasticsearch google-cloud-storage

USER airflow