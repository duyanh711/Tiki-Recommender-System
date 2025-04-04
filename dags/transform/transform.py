import re
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, from_unixtime, udf, isnan
from pyspark.sql.types import IntegerType, StringType
from config import settings
from extract.minio_manager import MinIOHandler
from src.utils.logger import setup_logger
from transform.tiki_transformer import TikiTransformer

logger = setup_logger(__name__)

minio_config = {
            "endpoint": settings.MINIO_CONFIG["endpoint_url"],
            "access_key": settings.MINIO_CONFIG["aws_access_key_id"],
            "secret_key": settings.MINIO_CONFIG["aws_secret_access_key"],
            "bucket": "warehouse"
        }

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

minio_endpoint_url = os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000")
minio_access_key_val = os.getenv("MINIO_ROOT_USER", "minio")
minio_secret_key_val = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
silver_bucket_name = MINIO_BUCKET

def _create_spark_session(app_name: str) -> SparkSession:
    """Tạo và cấu hình SparkSession để kết nối MinIO."""
    logger.info(f"Creating Spark session: {app_name}")
    return SparkSession.builder \
        .appName(app_name) \
        .master(SPARK_CONFIG.get("master", "local[*]")) \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint_url) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key_val) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key_val) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def _get_raw_data(transformer: TikiTransform, data_type: str) -> Optional[pd.DataFrame]:
    """Lấy dữ liệu raw Pandas DF từ transformer."""
    logger.info(f"Fetching raw data for type: {data_type}...")
    try:
        if data_type == "categories":
            df = transformer.get_categories()
        else:
            df = transformer.transform_data(type=data_type)

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
        # Có thể raise lỗi ở đây nếu XCom là bắt buộc:
        # raise ValueError(f"Input path for task '{task_id}' not found in XComs!")

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

def _write_output_parquet(df: DataFrame, output_path: str) -> int:
    """Ghi Spark DataFrame ra Parquet."""
    logger.info(f"Writing transformed data to {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    count = df.count() # Lấy count sau khi ghi có thể không chính xác nếu có lỗi, nên lấy trước hoặc bỏ qua
    # count = spark.read.parquet(output_path).count() # Cách chính xác hơn để lấy count sau khi ghi
    logger.info(f"Successfully wrote data to {output_path}.") # Ghi số lượng nếu bạn lấy count
    # Tạm thời trả về 0 vì count() có thể tốn kém
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


# --- TASK FUNCTIONS ĐÃ REFACTOR ---

def transform_sellers_task(ti=None, **context):
    """Task transform sellers đã refactor."""
    task_name = "Sellers"
    data_type = "sellers"
    output_suffix = "sellers"
    spark = None
    output_path = f"s3a://{silver_bucket_name}/silver/tiki/{output_suffix}/"
    final_count = 0
    success = False

    try:
        spark = _create_spark_session(f"AirflowTiki{task_name}")
        transformer = TikiTransform(MINIO_CONFIG)
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
    output_path = f"s3a://{silver_bucket_name}/silver/tiki/{output_suffix}/"
    final_count = 0
    success = False

    try:
        spark = _create_spark_session(f"AirflowTiki{task_name}")
        transformer = TikiTransform(MINIO_CONFIG)
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
    upstream_default_path = f"s3a://{silver_bucket_name}/silver/tiki/sellers/"
    spark = None
    output_path = f"s3a://{silver_bucket_name}/silver/tiki/{output_suffix}/"
    final_count = 0
    success = False

    try:
        spark = _create_spark_session(f"AirflowTiki{task_name}")
        transformer = TikiTransform(MINIO_CONFIG)
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
    upstream_default_path = f"s3a://{silver_bucket_name}/silver/tiki/products/"
    spark = None
    output_path = f"s3a://{silver_bucket_name}/silver/tiki/{output_suffix}/"
    final_count = 0
    success = False

    try:
        spark = _create_spark_session(f"AirflowTiki{task_name}")
        transformer = TikiTransform(MINIO_CONFIG)
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