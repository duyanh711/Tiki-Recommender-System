import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler
from config.settings import LOG_LEVEL

class MinIOHandlerLogging(logging.Handler):
    """
    Custom logging handler to store logs in MinIO using the existing MinIOHandler.
    """
    def __init__(self, minio_handler, log_dir="logs/tiki/", filename="app.log"):
        super().__init__()
        self.minio_handler = minio_handler  # Use the provided MinIOHandler instance
        self.log_dir = log_dir
        self.filename = filename
        self.buffer = []  # Buffer to store logs before uploading
        self.buffer_size = 100  # Number of logs before uploading to MinIO

    def emit(self, record):
        """
        Write log to buffer and upload to MinIO when buffer is full.
        Args:
            record: Log record.
        """
        try:
            log_entry = self.format(record)
            self.buffer.append(log_entry)

            if len(self.buffer) >= self.buffer_size:
                self._upload_to_minio()
        except Exception as e:
            print(f"Error in MinIOHandlerLogging: {str(e)}")

    def _upload_to_minio(self):
        """
        Upload logs from buffer to MinIO using MinIOHandler.
        """
        if not self.buffer:
            return

        try:
            # Create log content from buffer
            log_content = "\n".join(self.buffer) + "\n"

            # Create file name with timestamp
            timestamp = datetime.now().strftime("%Y%m%d")
            object_name = f"{self.log_dir}{timestamp}/{self.filename}"

            # Use MinIOHandler to upload the log content as a text file
            # Since MinIOHandler expects JSON or CSV, we'll write the log to a temp file first
            tmp_file_path = os.path.join(self.minio_handler.tmp_dir, "temp_log.txt")
            with open(tmp_file_path, "w", encoding="utf-8") as f:
                f.write(log_content)

            # Upload to MinIO using MinIOHandler (we'll treat it as a JSON file for simplicity)
            self.minio_handler.put_file_to_minio({"log": log_content}, object_name, file_type="json")

            # Clear buffer after upload
            self.buffer = []
        except Exception as e:
            print(f"Error uploading log to MinIO: {str(e)}")
        finally:
            # Clean up temporary file if it exists
            if os.path.exists(tmp_file_path):
                os.remove(tmp_file_path)

    def close(self):
        """
        Ensure remaining logs in buffer are uploaded when handler is closed.
        """
        self._upload_to_minio()
        super().close()

def setup_logger(name, minio_handler=None, log_to_file=True, log_to_minio=True):
    """
    Set up a logger with handlers for console, file, and MinIO.
    Args:
        name (str): Name of the logger.
        minio_handler (MinIOHandler): Instance of MinIOHandler for MinIO logging.
        log_to_file (bool): Whether to log to a file.
        log_to_minio (bool): Whether to log to MinIO.
    Returns:
        logging.Logger: Configured logger.
    """
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, LOG_LEVEL))

    # Avoid adding handlers if logger already has them
    if logger.handlers:
        return logger

    # Define log format
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Handler for console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Handler for file (RotatingFileHandler to limit file size)
    if log_to_file:
        os.makedirs("/opt/airflow/logs", exist_ok=True)
        file_handler = RotatingFileHandler(
            "/opt/airflow/logs/app.log",
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # Handler for MinIO
    if log_to_minio and minio_handler:
        minio_logging_handler = MinIOHandlerLogging(minio_handler, log_dir="logs/tiki/")
        minio_logging_handler.setFormatter(formatter)
        logger.addHandler(minio_logging_handler)

    return logger