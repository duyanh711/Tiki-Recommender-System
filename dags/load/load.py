import os
import re
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, from_unixtime, udf, isnan, explode
from pyspark.sql.types import IntegerType, StringType, TimestampType
from config import settings
from extract.minio_manager import MinIOHandler
from src.utils.logger import setup_logger
from transform.tiki_transformer import TikiTransformer
from typing import Dict, List, Optional, Tuple, Callable, Any
from load.postgres_handler import PostgresHandler
from transform.transform import create_spark_session, read_parquet_from_path
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = setup_logger(__name__)

MINIO_CONFIG = {
            "endpoint": settings.MINIO_CONFIG["endpoint_url"],
            "access_key": settings.MINIO_CONFIG["aws_access_key_id"],
            "secret_key": settings.MINIO_CONFIG["aws_secret_access_key"],
            "bucket": "warehouse"
        }

DB_CONFIG = {
    "host": "de_psql",
    "port": 5432,
    "database": "tiki_recommender",
    "user": "airflow",
    "password": "airflow"
}

SPARK_SETTINGS = settings.SPARK_CONFIG
SPARK_MASTER = SPARK_SETTINGS.get("master", "local[*]")
SPARK_APP_NAME_PREFIX = SPARK_SETTINGS.get("appNamePrefix", "AirflowTikiApp")

SPARK_CONFIG = {"master": SPARK_MASTER}

spark = SparkSession.builder
jdbc_driver_version = "42.7.3"
jdbc_package = f"org.postgresql:postgresql:{jdbc_driver_version}"


def create_db_if_not_exists():
    hook = PostgresHook(postgres_conn_id='postgres_de')
    conn = hook.get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM pg_database WHERE datname='tiki_recommender'")
    exists = cur.fetchone()
    if not exists:
        cur.execute("CREATE DATABASE tiki_recommender")
        print("Database tiki_recommender created.")
    else:
        print("Database tiki_recommender already exists.")

    cur.close()
    conn.close()


def load_gold_to_pg():
    task_name = "LoadAllGoldTablesToPg"
    gold_base_path = f"s3a://warehouse/gold/tiki/"
    write_mode = "overwrite"
    spark = None
    loaded_tables_info = {}
    logger.info(f"Starting task: {task_name}")
    logger.info(f"Scanning Gold layer base path: {gold_base_path}")

    try:
        spark = create_spark_session(f"{SPARK_APP_NAME_PREFIX}{task_name}")
        pg_handler = PostgresHandler(DB_CONFIG)
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI(gold_base_path), hadoop_conf)
        hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(gold_base_path)

        discovered_dirs = []

        try:
            # ListStatus trả về mảng FileStatus
            statuses = fs.listStatus(hadoop_path)
            for status in statuses:
                if status.isDirectory():
                    dir_name = status.getPath().getName()
                    discovered_dirs.append(dir_name)
            logger.info(f"Discovered directories in Gold layer: {discovered_dirs}")
        except Exception as e_list:
            # Xử lý lỗi nếu không liệt kê được thư mục (ví dụ: path không tồn tại)
            if "FileNotFoundException" in str(e_list) or "Path does not exist" in str(e_list):
                 logger.error(f"Gold base path not found or empty: {gold_base_path}. Cannot list tables.")
                 # Có thể kết thúc task ở đây hoặc tiếp tục (sẽ không load gì)
                 return {"loaded_tables": {}} # Trả về kết quả rỗng
            else:
                 logger.error(f"Error listing directories in {gold_base_path}: {e_list}", exc_info=True)
                 raise # Raise lỗi để Airflow biết

        if not discovered_dirs:
            logger.warning(f"No subdirectories found in {gold_base_path}. Nothing to load.")
            return {"loaded_tables": {}}

        for dir_name in discovered_dirs:
            parquet_path = f"{gold_base_path}{dir_name}/"
            postgres_table_name = f"{dir_name}"
            logger.info(f"--- Processing table: {dir_name} ---")
            logger.info(f"Reading from: {parquet_path}")
            logger.info(f"Targeting Postgres table: {postgres_table_name}")

            try:
                gold_df = read_parquet_from_path(spark, parquet_path)

                if gold_df is not None:
                    pg_handler.write_spark_df(gold_df, postgres_table_name, write_mode)
                    loaded_tables_info[postgres_table_name] = {"source_path": parquet_path, "status": "Success"}
                else:
                    logger.warning(f"Data for table '{dir_name}' at {parquet_path} is empty. Skipping load.")
                    loaded_tables_info[postgres_table_name] = {"source_path": parquet_path, "status": "Skipped (Empty Source)"}

            except Exception as e_load:
                logger.error(f"Failed to load table '{dir_name}' from {parquet_path} to {postgres_table_name}: {e_load}", exc_info=True)
                loaded_tables_info[postgres_table_name] = {"source_path": parquet_path, "status": f"Failed: {e_load}"}

        logger.info(f"Finished loading all discovered tables. Summary: {loaded_tables_info}")

        return {"loaded_tables": loaded_tables_info}

    except Exception as e:
        logger.error(f"An error occurred during the overall {task_name} task: {e}", exc_info=True)
        raise
    finally:
        if spark:
            logger.info(f"Stopping Spark session for {task_name}.")
            spark.stop()

