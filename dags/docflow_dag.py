import sys

sys.path.insert(0, "/opt/airflow")

from datetime import datetime as dt

from airflow import DAG
from airflow.operators.python import PythonOperator

from data_quality.validate_bronze import run_validation
from ingestion.ingest_arxiv import run_ingestion
from processing.embad_and_store import run_embedding
from processing.process_silver import run_processing

with DAG(
    dag_id="docflow_pipeline",
    start_date=dt(2026, 1, 1),
    schedule="0 6 * * 1-5",
    catchup=False,
    tags=["docflow", "arxiv", "nlp"],
) as dag:
    ingest = PythonOperator(task_id="ingest_arxiv", python_callable=run_ingestion)
    validate = PythonOperator(task_id="validate_bronze", python_callable=run_validation)
    process = PythonOperator(
        task_id="silver_processing", python_callable=run_processing
    )
    embadding = PythonOperator(task_id="embed_and_store", python_callable=run_embedding)

    ingest >> validate >> process >> embadding
