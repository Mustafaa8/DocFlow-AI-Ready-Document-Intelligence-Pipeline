import sys
sys.path.insert(0, "/opt/airflow")

from datetime import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator

def ingest():
    from ingestion.ingest_arxiv import run_ingestion
    run_ingestion()

def validate():
    from data_quality.validate_bronze import run_validation
    run_validation()

def process():
    from processing.process_silver import run_processing
    run_processing()

def embed():
    from processing.embad_and_store import run_embedding
    run_embedding()

with DAG(
    dag_id="docflow_pipeline",
    start_date=dt(2026, 1, 1),
    schedule="0 6 * * 1-5",
    catchup=False,
    tags=["docflow", "arxiv", "nlp"],
) as dag:

    ingest_task = PythonOperator(task_id="ingest_arxiv", python_callable=ingest)
    validate_task = PythonOperator(task_id="validate_bronze", python_callable=validate)
    process_task = PythonOperator(task_id="process_silver", python_callable=process)
    embed_task = PythonOperator(task_id="embed_and_store", python_callable=embed)

    ingest_task >> validate_task >> process_task >> embed_task
