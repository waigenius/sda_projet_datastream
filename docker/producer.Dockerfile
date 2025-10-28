FROM python:3.11-slim

RUN pip install --no-cache-dir confluent-kafka==2.4.0 fastjsonschema==2.19.1

WORKDIR /app
COPY producer/ /app/producer/
COPY data/ /app/data/
ENV PYTHONDONTWRITEBYTECODE=1