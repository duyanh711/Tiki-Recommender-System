from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extract.extract import extract_from_tiki
from transform.transform import transform_categories_task, transform_products_task, transform_reviews_task, transform_sellers_task, \
                                build_product_gold_layer_task, build_review_gold_layer_task, build_gold_categories_task, build_gold_sellers_task

from load.load import create_db_if_not_exists, load_gold_to_pg

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

    # transform_sellers = PythonOperator(
    #     task_id="transform_sellers_task",
    #     python_callable=transform_sellers_task
    # )

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

    # build_products_gold_layer = PythonOperator(
    #     task_id="build_product_gold_layer_task",
    #     python_callable=build_product_gold_layer_task
    # )

    # build_reviews_gold_layer = PythonOperator(
    #     task_id="build_review_gold_layer_task",
    #     python_callable=build_review_gold_layer_task
    # )

    # build_categories_gold_layer = PythonOperator(
    #     task_id="build_category_gold_layer_task",
    #     python_callable=build_gold_categories_task
    # )

    
    # build_sellers_gold_layer = PythonOperator(
    #     task_id="build_seller_gold_layer_task",
    #     python_callable=build_gold_sellers_task
    # )

    # create_recommender_db = PythonOperator(
    #     task_id="create_tiki_recommender_db",
    #     python_callable=create_db_if_not_exists
    # )

    # create_tables = PostgresOperator(
    #     task_id="create_tables_in_tiki_recommender",
    #     postgres_conn_id="tiki_recommender_conn",
    #     sql="create_table.sql",
    #     autocommit=True
    # )

    load_gold_layer_to_db = PythonOperator(
        task_id="load_gold_layer_to_db_task",
        python_callable=load_gold_to_pg
    )

    # extract_task >> [transform_sellers, transform_categories]
    # transform_sellers >> transform_products >> transform_reviews

    # transform_reviews

    # build_products_gold_layer
    # build_gold_categories_task, build_gold_sellers_task
    # create_recommender_db

    # create_tables
    load_gold_to_pg