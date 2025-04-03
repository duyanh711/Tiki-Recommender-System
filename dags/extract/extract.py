from config import settings
from extract.minio_manager import MinIOHandler
from src.utils.logger import setup_logger
from tiki_crawler import TikiCrawler

logger = setup_logger(__name__)


def extract_from_tiki():
    """
    Main function to crawl data from Tiki and store in MinIO.
    Returns:
        list: List of collected products.
    """
    try:
        minio_config = {
            "endpoint": settings.MINIO_ENDPOINT,
            "access_key": settings.MINIO_ACCESS_KEY,
            "secret_key": settings.MINIO_SECRET_KEY,
            "secure": settings.MINIO_SECURE,
            "bucket": settings.MINIO_BUCKET
        }
        
        # Create MinIOHandler instance
        minio_handler = MinIOHandler(minio_config, tmp_dir="/opt/airflow/data")
        
        # Setup logger with MinIOHandler
        logger = setup_logger(__name__, minio_handler=minio_handler)
        
        crawler = TikiCrawler(minio_config, root_dir="bronze/tiki/", tmp_dir="/opt/airflow/data")
        products = crawler.scrape_all(page=1)  # Collect 1 page for testing
        total_products = crawler.num_products()
        logger.info(f"Total products collected: {total_products}")
        return products
    except Exception as e:
        logger.error(f"Error during extraction: {str(e)}")
        raise