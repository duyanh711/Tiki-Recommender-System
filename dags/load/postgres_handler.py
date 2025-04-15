import os
from src.utils.logger import setup_logger
from typing import Optional, Any, Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import ArrayType, StructType
from pyspark.sql.functions import col, to_json, when

logger = setup_logger(__name__)

class PostgresHandler:

    def __init__(self, postgres_config: Optional[Dict[str, str]] = None):
        if postgres_config:
            self.host = postgres_config.get("host")
            self.port = postgres_config.get("port")
            self.database = postgres_config.get("database")
            self.user = postgres_config.get("user")
            self.password = postgres_config.get("password")
        else:
            self.host = os.getenv("POSTGRES_HOST")
            self.port = os.getenv("POSTGRES_PORT")
            self.database = os.getenv("POSTGRES_DB")
            self.user = os.getenv("POSTGRES_USER")
            self.password = os.getenv("POSTGRES_PASSWORD")
        self.jdbc_url = f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
        self.connection_props = {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver"
        }

        logger.info(f"PostgresHandler initialized for {self.user}@{self.host}:{self.port}/{self.database}")
        

    def _prepare_df_for_jdbc(self, df: DataFrame):
        logger.debug(f"Preparing DataFrame schema for JDBC write:")
        df.printSchema()

        cols_to_convert = []
        for field in df.schema.fields:
            if isinstance(field.dataType, (ArrayType, StructType)):
                cols_to_convert.append(field.name)

        if cols_to_convert:
            logger.warning(f"Converting complex columns to JSON string for Postgres: {cols_to_convert}")
            for col_name in cols_to_convert:
                df = df.withColumn(col_name, when(col(col_name).isNotNull(), to_json(col(col_name))).otherwise(None))
            logger.debug("Schema after converting complex types:")
        else:
            logger.debug("No complex types found requiring conversion.")

        return df
    

    def write_spark_df(self, df: DataFrame, table_name: str, mode: str = "overwrite"):
        """
        Ghi Spark DataFrame vào bảng PostgreSQL chỉ định.
        Args:
            df (DataFrame): Spark DataFrame cần ghi.
            table_name (str): Tên bảng đích trong PostgreSQL.
            mode (str): Chế độ ghi ('overwrite', 'append', 'ignore', 'errorifexists').
        """
        record_count = df.count() # Lấy count trước khi ghi
        logger.info(f"Writing {record_count} records to PostgreSQL table '{table_name}' using mode '{mode}'...")

        if record_count == 0:
            logger.warning(f"Input DataFrame for table '{table_name}' is empty. Skipping write.")
            return # Không ghi gì nếu DataFrame rỗng

        try:
            df_to_write = self._prepare_df_for_jdbc(df)

            df_to_write.write.jdbc(
                url=self.jdbc_url,
                table=table_name,
                mode=mode,
                properties=self.connection_props
            )
            logger.info(f"Successfully wrote {record_count} records to table '{table_name}'.")
        except Exception as e:
            logger.error(f"Failed to write data to PostgreSQL table '{table_name}'. Error: {e}", exc_info=True)
            logger.error("DataFrame Schema potentially causing issue:")
            try:
                df_to_write.printSchema()
            except Exception as e_schema:
                logger.error(f"Could not print schema: {e_schema}")
            raise


            