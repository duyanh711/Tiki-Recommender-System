from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extract.extract import extract_from_tiki
from transform.transform import transform_categories_task, transform_products_task, transform_reviews_task, transform_sellers_task

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
    # extract_task = PythonOperator(
    #     task_id="extract_task",
    #     python_callable=extract_from_tiki,
    # )

    transform_sellers = PythonOperator(
        task_id="transform_sellers_task",
        python_callable=transform_sellers_task
    )

    # transform_categories = PythonOperator(
    #     task_id="transform_categories_task",
    #     python_callable=transform_categories_task
    # )

    # transform_products = PythonOperator(
    #     task_id="transform_products_task",
    #     python_callable=transform_products_task
    # )

    # transform_reviews = PythonOperator(
    #     task_id="transform_reviews_task",
    #     python_callable=transform_reviews_task
    # )

    # extract_task >> [transform_sellers, transform_categories]
    # transform_sellers >> transform_products >> transform_reviews

    transform_sellers