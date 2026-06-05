import json
import os
import hashlib
import tempfile
from datetime import datetime, timezone
from src.utils.logger import get_logger
from src.utils.minio_clients import MinIOClient
from src.utils.config import MINIO_RAW_BUCKET

logger = get_logger(__name__)


class MetadataManager:
    """
    Metadata manager for idempotent ingestion.
    Tracks: processed datasets, file hashes, timestamps, upload counts, and status.
    """

    def __init__(self):
        self.minio = MinIOClient()


    def dataset_key(self, country: str, dataset_id: str) -> str:
        """
        Standardizes the key, e.g., SPAIN_2024
        SPAIN_2024_LANDING    
        """
        return f"{country.upper()}_{dataset_id.upper()}"


    def metadata_object_name(self, country: str, dataset_id: str) -> str:
        """Builds the S3 path: _metadata/SPAIN_2024_LANDING.json"""
        key = self.dataset_key(country, dataset_id)
        return f"_metadata/{key}.json"


    def load(self, country: str, dataset_id: str) -> dict:
        """Loads metadata JSON from MinIO. If missing or fails, returns empty dict."""
        METADATA_OBJECT = self.metadata_object_name(country, dataset_id)
        temp_path = None
        
        try:
            # Create a temp file but don't hold it open so we can read it after download
            fd, temp_path = tempfile.mkstemp(suffix=".json")
            os.close(fd) # Close file descriptor immediately

            self.minio.download_file(
                bucket_name=MINIO_RAW_BUCKET,
                object_name=METADATA_OBJECT,
                file_path=temp_path
            )
            
            with open(temp_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            logger.debug(f"Metadata loaded successfully for {country.upper()}_{dataset_id.upper()}")
            return data
        except Exception:
            # Silent fallback is intentional for first-run idempotency
            logger.info(f"No metadata found for {country.upper()}_{dataset_id.upper()}. Starting fresh download.")
            return {}
        finally:
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)

    def save(self, country: str, dataset_id: str, metadata: dict):
        """Saves metadata JSON to MinIO with proper cleanup."""
        METADATA_OBJECT = self.metadata_object_name(country, dataset_id)
        temp_path = None
        
        try:
            # delete=False is necessary to allow upload_file to access the path
            with tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".json",
                delete=False,
                encoding="utf-8"
            ) as tmp:
                json.dump(metadata, tmp, indent=2)
                temp_path = tmp.name

            self.minio.upload_file(
                bucket_name=MINIO_RAW_BUCKET,
                object_name=METADATA_OBJECT,
                file_path=temp_path
            )
            logger.debug(f"Metadata saved successfully: {METADATA_OBJECT}")

        finally:
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)

    def compute_hash(self, file_path: str) -> str:
        """Computes MD5 hash of file in chunks to handle large datasets efficiently."""
        hasher = hashlib.md5()
        with open(file_path, "rb") as f:
            while chunk := f.read(8192):
                hasher.update(chunk)
        return hasher.hexdigest()


    def is_processed(self, country: str, dataset_id: str, file_hash: str) -> bool:
        """Checks if a dataset with the matching hash has already been completed."""
        metadata = self.load(country, dataset_id)
        if not metadata:
            return False
        
        return (metadata.get("status") == "completed" and metadata.get("hash") == file_hash)


    def mark_processed(self, country: str, dataset_id: str, file_hash: str, files_uploaded: int = 1):
        """Records a successful ingestion event."""
        metadata = {
            "status": "completed",
            "hash": file_hash,
            "files_uploaded": files_uploaded,
            "uploaded_at": datetime.now(timezone.utc).isoformat()
        }
        self.save(country, dataset_id, metadata)
        logger.info(f"Marked processed: {country.upper()} {dataset_id.upper()}")


    def mark_failed(self, country: str, dataset_id: str, reason: str):
        """Records a failure for debugging and visibility."""
        metadata = {
            "status": "failed",
            "reason": reason,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        self.save(country, dataset_id, metadata)
        logger.warning(f"Marked failed: {country.upper()} {dataset_id.upper()}")

    def get_record(self, country: str, dataset_id: str) -> dict:
        """
        Retrieve metadata record for a dataset.

        Examples:
        2024_LANDING
        2024_BRONZE
        2024_SILVER
        """

        return self.load(country, dataset_id)


    def list_all(self) -> list:
        """Returns a list of all metadata object names available."""
        return self.minio.list_object_names(
            bucket_name=MINIO_RAW_BUCKET,
            prefix="_metadata/"
        )