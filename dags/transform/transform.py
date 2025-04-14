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

logger = setup_logger(__name__)

MINIO_CONFIG = {
            "endpoint": settings.MINIO_CONFIG["endpoint_url"],
            "access_key": settings.MINIO_CONFIG["aws_access_key_id"],
            "secret_key": settings.MINIO_CONFIG["aws_secret_access_key"],
            "bucket": "warehouse"
        }
SPARK_SETTINGS = settings.SPARK_CONFIG
SPARK_MASTER = SPARK_SETTINGS.get("master", "local[*]")
SPARK_APP_NAME_PREFIX = SPARK_SETTINGS.get("appNamePrefix", "AirflowTikiApp")

SPARK_CONFIG = {"master": SPARK_MASTER}

spark = SparkSession.builder

@udf(IntegerType())
def convert_warranty_period(warranty):
    if not isinstance(warranty, str) or warranty.strip() == '':
        return 0
    warranty = warranty.lower().strip()
    if 'trọn đời' in warranty:
        return 100*365 
    if re.search(r'không|no warranty|0', warranty):
        return 0
    match_year = re.search(r'(\d+)\s*(năm|year)', warranty)
    match_month = re.search(r'(\d+)\s*(tháng|month)', warranty)
    match_day = re.search(r'(\d+)\s*(ngày|day)', warranty)
    if match_year:
        years = int(match_year.group(1))
        return years * 365  
    if match_month:
        months = int(match_month.group(1))
        return months * 30  
    if match_day:
        days = int(match_day.group(1))
        return days  
    logger.warning(f"Could not parse warranty string: '{warranty}'. Returning 0 day.")
    return 0
    
@udf(IntegerType())
def convert_to_days(joined_time):
    if joined_time is None:
        return 0
    match = re.search(r"(\d+)\s+(năm|tháng|ngày)", joined_time)
    if match:
        value, unit = int(match.group(1)), match.group(2)
        if unit == "năm":
            return value * 365  
        elif unit == "tháng":
            return value * 30  
        elif unit == "ngày":
            return value
    return 0

    '''
    4 Main functions to tranform data from bronze data set in MinIO
    Convert json to dataframe and convert some columns to correct data type
    Returns:
        DataFrame: Data frame of all data
    '''  


def _create_spark_session(app_name: str) -> SparkSession:
    """Tạo và cấu hình SparkSession để kết nối MinIO."""
    logger.info(f"Creating Spark session: {app_name}")
    return SparkSession.builder \
        .appName(app_name) \
        .master(SPARK_CONFIG.get("master", "local[*]")) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
        .config("spark.sql.catalogImplementation", "in-memory") \
        .config("spark.jars.packages",
                f"org.apache.hadoop:hadoop-aws:3.3.4,"
                f"com.amazonaws:aws-java-sdk-bundle:1.12.367") \
        .getOrCreate()

def _get_raw_data(transformer: TikiTransformer, data_type: str) -> Optional[pd.DataFrame]:
    """Lấy dữ liệu raw Pandas DF từ transformer."""
    logger.info(f"Fetching raw data for type: {data_type}...")
    try:
        if data_type == "categories":
            df = transformer.get_categories()
        else:
            df = transformer.transform_data(data_type)

        if df is None or df.empty:
            logger.warning(f"Raw data for type '{data_type}' is empty or failed to load.")
            return None
        logger.info(f"Fetched {len(df)} raw records for type '{data_type}'.")
        return df
    except Exception as e:
        logger.error(f"Failed to fetch raw data for type '{data_type}': {e}", exc_info=True)
        raise # Hoặc trả về None tùy chiến lược xử lý lỗi

def _read_upstream_parquet(spark: SparkSession, ti: Optional[Any], task_id: str, default_path: str) -> Optional[DataFrame]:
    """Đọc dữ liệu Parquet từ output của task trước đó."""
    input_path = None
    if ti:
        input_path = ti.xcom_pull(task_ids=task_id, key='output_path')

    if not input_path:
        input_path = default_path
        logger.warning(f"Could not pull path from XCom for task '{task_id}', using default: {input_path}")

    logger.info(f"Reading upstream data from: {input_path}")
    try:
        df = spark.read.parquet(input_path)
        logger.info("Upstream data read successfully.")
        return df
    except Exception as e:
        # Xử lý lỗi không tìm thấy file hoặc lỗi đọc khác
        logger.error(f"Failed to read upstream data from {input_path}: {e}", exc_info=True)
        # Tùy chọn: trả về None hoặc raise lỗi
        # return None
        raise


def _read_parquet_from_path(spark: SparkSession, input_path: str):
    logger.info(f"Reading data directly from fixed path: {input_path}")
    
    try:
        df = spark.read.parquet(input_path)
        if df.first() is None:
            logger.warning(f"Data read from {input_path} is empty.")
            return None
        else:
            logger.info(f"Data read successfully from {input_path}")
            return df
    except Exception as e:
        error_str = str(e)
        if "Path does not exist" in error_str or "FileNotFoundException" in error_str:
            logger.error(f"CRITICAL: Input path does not exist: {input_path}.")
        else:
            logger.error(f"CRITICAL: Failed to read data from {input_path}: {e}", exc_info=True)
        raise ValueError(f"Critical error: Failed to read required input data from {input_path}") from e


def _write_output_parquet(df: DataFrame, output_path: str) -> int:
    """Ghi Spark DataFrame ra Parquet."""
    logger.info(f"Writing transformed data to {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    count = df.count() # Lấy count sau khi ghi có thể không chính xác nếu có lỗi, nên lấy trước hoặc bỏ qua
    logger.info(f"Successfully wrote data to {output_path} with {count} records.") # Ghi số lượng nếu bạn lấy count
    return 0 # Hoặc tính count trước khi ghi nếu cần


def _push_xcoms_result(ti: Optional[Any], output_path: Optional[str], record_count: Optional[int]):
    """Push output path và record count lên XComs."""
    if ti:
        if output_path is not None:
            ti.xcom_push(key="output_path", value=output_path)
        if record_count is not None:
            # Lấy record_count chính xác hơn nếu cần (ví dụ từ _write_output)
            # Ở đây đang dùng count ước lượng hoặc 0
            ti.xcom_push(key="record_count", value=record_count if record_count is not None else 0)
        logger.info(f"Pushed XComs: output_path={output_path}, record_count={record_count}")


def transform_sellers_task(ti=None, **context):
    """Task transform sellers đã refactor."""
    task_name = "Sellers"
    data_type = "sellers"
    output_suffix = "sellers"
    spark = None
    output_path = f"s3a://{MINIO_CONFIG['bucket']}/silver/tiki/{output_suffix}/"
    final_count = 0
    success = False

    try:
        spark = _create_spark_session(f"AirflowTiki{task_name}")
        transformer = TikiTransformer(MINIO_CONFIG)
        pandas_df = _get_raw_data(transformer, data_type)

        if pandas_df is not None:
            sellers_spark_df = spark.createDataFrame(pandas_df)

            # --- LOGIC BIẾN ĐỔI RIÊNG CỦA SELLERS ---
            logger.info("Applying specific transformations for Sellers...")
            sellers_spark_df = sellers_spark_df.dropDuplicates(["seller_id"])
            sellers_spark_df = sellers_spark_df.filter(col("seller_id").isNotNull() & (col("seller_id") != 0))
            sellers_spark_df = sellers_spark_df.fillna(0, subset=["review_count", "total_follower", "days_since_joined"])
            sellers_spark_df = sellers_spark_df.fillna(0.0, subset=["avg_rating_point"])
            # -----------------------------------------

            final_count = sellers_spark_df.count() # Lấy count trước khi ghi
            _write_output_parquet(sellers_spark_df, output_path)
            success = True
            logger.info(f"Successfully transformed {final_count} {task_name} records.")
        else:
            logger.warning(f"Skipping write for {task_name} due to empty raw data.")
            output_path = None # Không có output path nếu không ghi gì

        _push_xcoms_result(ti, output_path, final_count if success else 0)
        # Trả về dict để dễ debug từ Airflow UI logs
        return {"output_path": output_path, "record_count": final_count if success else 0}

    except Exception as e:
        logger.error(f"An error occurred during {task_name} transformation: {e}", exc_info=True)
        raise
    finally:
        if spark:
            logger.info(f"Stopping Spark session for {task_name}.")
            spark.stop()

def transform_categories_task(ti=None, **context):
    """Task transform categories đã refactor."""
    task_name = "Categories"
    data_type = "categories"
    output_suffix = "categories"
    spark = None
    output_path = f"s3a://{MINIO_CONFIG['bucket']}/silver/tiki/{output_suffix}/"
    final_count = 0
    success = False

    try:
        spark = _create_spark_session(f"AirflowTiki{task_name}")
        transformer = TikiTransformer(MINIO_CONFIG)
        pandas_df = _get_raw_data(transformer, data_type)

        if pandas_df is not None:
            categories_spark_df = spark.createDataFrame(pandas_df)

            # --- LOGIC BIẾN ĐỔI RIÊNG CỦA CATEGORIES ---
            logger.info("Applying specific transformations for Categories...")
            # (Ví dụ: ép kiểu, đổi tên cột nếu cần)
            # categories_spark_df = categories_spark_df.withColumn("category_id", col("category_id").cast(IntegerType()))
            # ------------------------------------------

            final_count = categories_spark_df.count()
            _write_output_parquet(categories_spark_df, output_path)
            success = True
            logger.info(f"Successfully transformed {final_count} {task_name} records.")
        else:
            logger.warning(f"Skipping write for {task_name} due to empty raw data.")
            output_path = None

        _push_xcoms_result(ti, output_path, final_count if success else 0)
        return {"output_path": output_path, "record_count": final_count if success else 0}

    except Exception as e:
        logger.error(f"An error occurred during {task_name} transformation: {e}", exc_info=True)
        raise
    finally:
        if spark:
            logger.info(f"Stopping Spark session for {task_name}.")
            spark.stop()


def transform_products_task(ti=None, **context):
    """Task transform products đã refactor."""
    task_name = "Products"
    data_type = "products"
    output_suffix = "products"
    upstream_task_id = 'transform_sellers_task'
    upstream_default_path = f"s3a://{MINIO_CONFIG['bucket']}/silver/tiki/sellers/"
    spark = None
    output_path = f"s3a://{MINIO_CONFIG['bucket']}/silver/tiki/{output_suffix}/"
    final_count = 0
    success = False

    try:
        spark = _create_spark_session(f"AirflowTiki{task_name}")
        transformer = TikiTransformer(MINIO_CONFIG)
        pandas_df = _get_raw_data(transformer, data_type)

        if pandas_df is not None:
            products_spark_df = spark.createDataFrame(pandas_df)

            # Đọc dữ liệu upstream (sellers)
            silver_sellers_df = _read_upstream_parquet(spark, ti, upstream_task_id, upstream_default_path)
            if silver_sellers_df is None:
                raise ValueError(f"Upstream data from '{upstream_task_id}' could not be read.")
            silver_sellers_to_join = silver_sellers_df.select("seller_id").distinct()

            # --- LOGIC BIẾN ĐỔI RIÊNG CỦA PRODUCTS ---
            logger.info("Applying specific transformations for Products...")
            products_spark_df = products_spark_df.dropDuplicates(["product_id", "seller_id"])
            products_spark_df = products_spark_df.join(silver_sellers_to_join, on="seller_id", how="inner")
            # Xử lý brand
            products_spark_df = products_spark_df.withColumn("brand_id", when(isnan(col("brand_id")) | col("brand_id").isNull(), 0).otherwise(col("brand_id").cast(IntegerType())))
            products_spark_df = products_spark_df.withColumn("brand_name", when(isnan(col("brand_name")) | col("brand_name").isNull() | (col("brand_name") == ""), "No Brand").otherwise(col("brand_name")))
            # Xử lý warranty
            products_spark_df = products_spark_df.withColumn("warranty_period_days", convert_warranty_period(col("warranty_period")))
            products_spark_df = products_spark_df.withColumn("warranty_type", when(isnan(col("warranty_type")) | col("warranty_type").isNull() | (col("warranty_type") == ""), "Không bảo hành").otherwise(col("warranty_type")))
            products_spark_df = products_spark_df.withColumn("warranty_location", when(isnan(col("warranty_location")) | col("warranty_location").isNull() | (col("warranty_location") == ""), "Không bảo hành").otherwise(col("warranty_location")))
            # Xử lý return policy
            products_spark_df = products_spark_df.withColumn("return_policy", when(col("return_reason") == "any_reason", "Bất cứ lý do gì").when(col("return_reason") == "defective_product", "Sản phẩm hư hỏng").when((col("return_reason") == "no_return") | col("return_reason").isNull(), "Không đổi trả").otherwise(col("return_reason")))
            # Xử lý quantity sold
            products_spark_df = products_spark_df.fillna(0, subset=["quantity_sold"])
            products_spark_df = products_spark_df.withColumn("quantity_sold", col("quantity_sold").cast(IntegerType()))
            # Drop cột gốc nếu muốn
            # products_spark_df = products_spark_df.drop("warranty_period", "return_reason")
            # ------------------------------------------

            final_count = products_spark_df.count()
            _write_output_parquet(products_spark_df, output_path)
            success = True
            logger.info(f"Successfully transformed {final_count} {task_name} records.")
        else:
            logger.warning(f"Skipping write for {task_name} due to empty raw data.")
            output_path = None

        _push_xcoms_result(ti, output_path, final_count if success else 0)
        return {"output_path": output_path, "record_count": final_count if success else 0}

    except Exception as e:
        logger.error(f"An error occurred during {task_name} transformation: {e}", exc_info=True)
        raise
    finally:
        if spark:
            logger.info(f"Stopping Spark session for {task_name}.")
            spark.stop()

def transform_reviews_task(ti=None, **context):
    """Task transform reviews đã refactor."""
    task_name = "Reviews"
    data_type = "reviews"
    output_suffix = "reviews"
    upstream_task_id = 'transform_products_task'
    upstream_default_path = f"s3a://{MINIO_CONFIG['bucket']}/silver/tiki/products/"
    spark = None
    output_path = f"s3a://{MINIO_CONFIG['bucket']}/silver/tiki/{output_suffix}/"
    final_count = 0
    success = False

    try:
        spark = _create_spark_session(f"AirflowTiki{task_name}")
        transformer = TikiTransformer(MINIO_CONFIG)
        pandas_df = _get_raw_data(transformer, data_type)

        if pandas_df is not None:
            reviews_spark_df = spark.createDataFrame(pandas_df)

            # Đọc dữ liệu upstream (products)
            silver_products_df = _read_upstream_parquet(spark, ti, upstream_task_id, upstream_default_path)
            if silver_products_df is None:
                 raise ValueError(f"Upstream data from '{upstream_task_id}' could not be read.")
            silver_products_to_join = silver_products_df.select("product_id", "seller_id").distinct()

            # --- LOGIC BIẾN ĐỔI RIÊNG CỦA REVIEWS ---
            logger.info("Applying specific transformations for Reviews...")
            reviews_spark_df = reviews_spark_df.join(silver_products_to_join, on=["product_id", "seller_id"], how="inner")
            reviews_spark_df = reviews_spark_df.dropDuplicates(["review_id"])
            # Xử lý timestamps và joined_time
            reviews_spark_df = reviews_spark_df.withColumn("created_ts", when(col("created_at").isNotNull(), from_unixtime(col("created_at")).cast(TimestampType())).otherwise(None))
            reviews_spark_df = reviews_spark_df.withColumn("purchased_ts", when(col("purchased_at").isNotNull(), from_unixtime(col("purchased_at")).cast(TimestampType())).otherwise(None))
            reviews_spark_df = reviews_spark_df.withColumn("joined_days", convert_to_days(col("joined_time")))
            # Xử lý null/NaN counts
            reviews_spark_df = reviews_spark_df.withColumn("total_review", when(isnan(col("total_review")) | col("total_review").isNull(), 0).otherwise(col("total_review").cast(IntegerType())))
            reviews_spark_df = reviews_spark_df.withColumn("total_thank", when(isnan(col("total_thank")) | col("total_thank").isNull(), 0).otherwise(col("total_thank").cast(IntegerType())))
            # Drop cột gốc nếu muốn
            # reviews_spark_df = reviews_spark_df.drop("created_at", "purchased_at", "joined_time")
            # ------------------------------------------

            final_count = reviews_spark_df.count()
            _write_output_parquet(reviews_spark_df, output_path)
            success = True
            logger.info(f"Successfully transformed {final_count} {task_name} records.")
        else:
            logger.warning(f"Skipping write for {task_name} due to empty raw data.")
            output_path = None

        _push_xcoms_result(ti, output_path, final_count if success else 0)
        return {"output_path": output_path, "record_count": final_count if success else 0}

    except Exception as e:
        logger.error(f"An error occurred during {task_name} transformation: {e}", exc_info=True)
        raise
    finally:
        if spark:
            logger.info(f"Stopping Spark session for {task_name}.")
            spark.stop()

# Create gold layer
def build_product_gold_layer_task():
    task_name = "BronzeToProductGold"
    output_suffix = "products"
    spark = None
    input_path = f"s3a://{MINIO_CONFIG['bucket']}/silver/tiki/{output_suffix}/"
    output_path = f"s3a://{MINIO_CONFIG['bucket']}/gold/tiki"
    spark = None
    results = {}

    try:
        spark = _create_spark_session(f"AirflowTiki{task_name}")
        silver_products_df = _read_parquet_from_path(spark, input_path)

        if silver_products_df is None:
            logger.warning(f"Input silver_products is empty or could not be read from {input_path}. Skipping {task_name}.")
            return {"outputs": results}
        
        select_columns_gold_products = ["product_id", "seller_id", "product_sku", "product_name", "product_url", "images_url", "description", "price", \
                                        "discount_rate", "inventory_status", "inventory_type", "quantity_sold", "rating_average", "review_count" \
                                        "specifications", "breadcrumbs", "category_id", "is_authentic", "is_freeship_xtra", "is_top_deal", "return_reason" \
                                        "brand_id", "authors", "warranty_type", "warranty_location", "warranty_period_days", "return_policy"]
        existing_select_columns = [c for c in select_columns_gold_products if c in silver_products_df.columns]
        gold_products = silver_products_df.select(*existing_select_columns)
        output_path_products = f"{output_path}/products/"
        count_products = _write_output_parquet(gold_products, output_path_products)
        results["products"] = {"output_path": output_path_products, "record_count": count_products}

        output_path_authors = f"{output_path}/authors/"
        if "authors" in silver_products_df.columns:
            logger.info("Processing 'authors' column...")
            try:
                exploded_authors_df = silver_products_df \
                    .filter(col("authors").isNotNull()) \
                    .withColumn("author_tuple", explode(col("authors"))) \
                    .select(
                        "product_id", "seller_id",
                        col("author_tuple._1").alias("author_id"),
                        col("author_tuple._2").alias("author_name")
                    )

                gold_authors = exploded_authors_df \
                    .select("author_id", "author_name") \
                    .filter(col("author_id").isNotNull()) \
                    .drop_duplicates(["author_id"])
                count_authors = _write_output_parquet(gold_authors, output_path_authors)
                results["authors"] = {"output_path": output_path_authors, "record_count": count_authors}
                logger.info(f"Created gold_authors with {count_authors} unique records.")
            except Exception as e_auth:
                logger.error(f"Failed processing 'authors' column (expected tuple access). Error: {e_auth}", exc_info=True)
        else:
            logger.warning("Column 'authors' not found in silver_products. Skipping author tables.")
        
        output_path_brands = f"{output_path}/brands/"
        results["brands"] = {"output_path": None, "record_count": 0}
        if "brand_id" in silver_products_df.columns:
            gold_brands = silver_products_df \
                .select("brand_id", "brand_name") \
                .filter(col("brand_id").isNotNull() & (col("brand_id") != 0)) \
                .drop_duplicates(["brand_id"])

            count_brands = _write_output_parquet(gold_brands, output_path_brands)
            results["brands"] = {"output_path": output_path_brands, "record_count": count_brands}
            logger.info(f"Created gold_brands with {count_brands} unique records.")
        else:
            logger.warning("Column 'brand_id' not found in silver_products. Skipping gold_brands.")

        logger.info(f"Finished {task_name}. Results: {results}")
        return results
    except Exception as e:
        logger.error(f"An error occurred during {task_name} transformation: {e}", exc_info=True)
        raise
    finally:
        if spark:
            logger.info(f"Stopping Spark session for {task_name}.")
            spark.stop()

    