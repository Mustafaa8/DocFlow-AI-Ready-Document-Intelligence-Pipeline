FROM apache/airflow:2.9.2

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    "pydantic==2.12.5" \
    "great-expectations==1.14.0" \
    "qdrant-client==1.13.3" \
    "polars==1.38.1" \
    "fastembed==0.7.4" \
    "loguru==0.7.3" \
    "pypdf==6.8.0" \
    "python-dotenv==1.2.2" \
    "requests==2.32.5" \
    "pyarrow==19.0.1"

