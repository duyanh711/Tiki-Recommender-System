import os
import json
from contextlib import contextmanager
from minio import Minio
import pandas as pd
import config

@contextmanager
def connect_minio(config):
    client = Minio(
        endpoint="minio:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
    try:
        yield client
    except Exception as e:
        print(f"MinIO connection error: {e}")
        raise

class MinioManager:
    def __init__(self, config, tmp_dir="./tmp", root_dir="bronze/tiki"):
        self._config = config
        self._temp_dir = tmp_dir
        self.root_dir = root_dir
        os.makedirs(self._temp_dir, exist_ok=True)

    def _get_minio_path(self, path):
        return os.path.join(self.root_dir, path)
    
    def put_file(self, file, path, file_type="json"):
        try:
            tmp_file_path = os.path.join(self._temp_dir, path)
            os.makedirs(os.path.dirname(tmp_file_path), exist_ok=True)

            if file_type == "json":  
                with open(tmp_file_path, "w", encoding="utf-8") as f:
                    json.dump(file, f, ensure_ascii=False, indent=4)
            elif file_type == "csv":  
                file.to_csv(tmp_file_path, index=False)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")

            with connect_minio(self._config) as client:
                minio_path = self._get_minio_path(path)
                client.fput_object(self._config["bucket"], minio_path, tmp_file_path)
                print(f"Successfully uploaded to MinIO: {minio_path}")

            os.remove(tmp_file_path)

        except Exception as e:
            print(f"Upload failed: {e}")
            raise
    
    def get_file(self, path, file_type="json"):
        try:
            tmp_file_path = os.path.join(self._temp_dir, path)
            os.makedirs(os.path.dirname(tmp_file_path), exist_ok=True)
            
            with connect_minio(self._config) as client:
                minio_path = self._get_minio_path(path)
                client.fget_object(self._config["bucket"], minio_path, tmp_file_path)

                if file_type == "json":
                    with open(tmp_file_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                elif file_type == "csv":
                    data = pd.read_csv(tmp_file_path)
                else:
                    raise ValueError(f"Unsupported file type: {file_type}")
                
                os.remove(tmp_file_path)
                return data
            
        except Exception as e:
            print(f"Download failed: {e}")
            raise
class MinIOHandler:

    def __init__(self, minio_config, tmp_dir = "./tmp", root_dir = "bronze/tiki"):
        self.minio_config = minio_config
        self._temp_dir = tmp_dir
        self.root_dir = root_dir
        
    def put_file_to_minio(self, file, path, file_type="json"):
        try:
            tmp_file_path = os.path.join(self._temp_dir, path)
            os.makedirs(os.path.dirname(tmp_file_path), exist_ok=True)
            if file_type == "json":  
                with open(tmp_file_path, "w", encoding="utf-8") as f:
                    json.dump(file, f, ensure_ascii=False, indent=4)
            elif file_type == "csv":  
                file.to_csv(tmp_file_path, index=False)

            with connect_minio(self.minio_config) as client:
                minio_path = os.path.join(self.root_dir, path)
                client.fput_object(self.minio_config["bucket"], minio_path, tmp_file_path)
                print(f"Successfully uploaded data to Minio at {path}")

            os.remove(tmp_file_path)

        except Exception as e:
            print(f"Failed to upload data to Minio at {path}: {e}")
            raise

    def get_file_from_minio(self, path, file_type="json"):
        try:
            tmp_file_path = os.path.join(self._temp_dir, path)
            os.makedirs(os.path.dirname(tmp_file_path), exist_ok=True)

            with connect_minio(self.minio_config) as client:
                if not path.startswith(self.root_dir):
                    minio_path = os.path.join(self.root_dir, path)
                else:
                    minio_path = path

                client.fget_object(self.minio_config["bucket"], minio_path, tmp_file_path)
                
                if file_type == "json":
                    with open(tmp_file_path, "r", encoding="utf-8") as f:
                        data = json.load(f)  
                elif file_type == "csv":
                    data = pd.read_csv(tmp_file_path)  

                else:
                    raise ValueError(f"Unsupported file type: {file_type}")

                os.remove(tmp_file_path)
                return data  
            
        except Exception as e:
            print(f"Failed to get file from Minio at {path}: {e}")
            raise

if __name__ == "__main__":
    print(config.get("endpoint_url"))