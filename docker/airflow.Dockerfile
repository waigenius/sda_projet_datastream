FROM apache/airflow:2.9.2-python3.11

# 1) ROOT pour apt + création du répertoire exports
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl jq && \
    rm -rf /var/lib/apt/lists/*

# Créer /exports en root puis l'assigner à l'utilisateur airflow
RUN mkdir -p /exports && chown -R airflow:root /exports

# 2) Revenir à l'utilisateur airflow pour pip install
USER airflow
RUN pip install --no-cache-dir \
    confluent-kafka==2.4.0 \
    elasticsearch==8.13.2 \
    pandas==2.2.2 \
    pyarrow==16.1.0 \
    python-dateutil==2.9.0.post0