from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
from src.utils.logger import setup_logger

from contextlib import contextmanager

logger = setup_logger(__name__)

@contextmanager
def connect_spark(config):
    try:
        spark = SparkSession.builder.appName("task-{}".format(datetime.today()))
        for key, value in config.items():
            if key == "master_url":
                spark = spark.master(value)
            elif key == "bucket":
                spark = spark.config(key, value)
        yield spark.getOrCreate()
    except Exception:
        raise

class SparkManager:
    def __init__(self, config):
        self._config = config

    def _get_path(self, layer, schema, table):
        base_dir = f"s3a://{self._config['bucket']}"
        file_name = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        file_path = f"{base_dir}/{file_name}.parquet"
        logger.info(f"File path: {file_path}")
        return file_path

    def load_input(self, spark: SparkSession, layer, schema, table):
        file_path = self._get_path(layer, schema, table)
        try:
            with connect_spark(self._config) as spark:
                logger.info("Loading Input Spark...")
                df = spark.read.parquet(file_path)
                return df
        except Exception:
            raise

    def handle_output(self, df: DataFrame, layer, schema, table):
        file_path = self._get_path(layer, schema, table)
        try:
            logger.info("Handling Output Spark...")
            if df:
                df.write.mode("overwrite").parquet(file_path)
            else:
                logger.info("No Output!")
        except Exception:
            raise
