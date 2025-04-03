from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extract.extract import extract_from_tiki

defaut_args = {
    "owner": "Gavin",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retries": False,
    "retries": 0
}

with DAG(
    "tiki_etl_pipeline",
    default_args=defaut_args,
    description="ETL pipeline for Tiki recommender system",
    schedule_interval=timedelta(days=1),
    start_date=datetime.now(),
    catchup=False,
) as dag:
    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract_from_tiki,
    )

    extract_task