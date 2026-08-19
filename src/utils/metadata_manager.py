import json
import os
import hashlib
import tempfile
from datetime import datetime, timezone
from src.utils.logger import get_logger
from src.utils.minio_clients import MinIOClient
from src.utils.config import MINIO_RAW_BUCKET
from typing import Any, Dict, Optional
from minio.error import S3Error
logger = get_logger(__name__)


class MetadataManager:
    """
    Metadata manager for idempotent ingestion.
    Tracks: processed datasets, file hashes, timestamps, upload counts, and status.

    metadata structure:
    temp/
    |__  _metadata /
         |
         |----- LANDING/
         |       |-----SPAIN.json
         |      |-----GERMANY.json
         |       |------IRELAND.json
         |
         |------ BRONZE/
                 |-----


     {
        "SPAIN_2023":{
            "dataset_id": "SPAIN_2023",
            "country": "SPAIN",
            "year": "2023",
            "stage": "SILVER",
            "status": "completed",
            .....
            },
            "SPAIN_2024":{
            .....
            }
     }
    """

    def __init__(self):
        self.minio = MinIOClient()
          


    def dataset_key(self, country:str, stage:str) -> str:
        """
        Standardizes the key, e.g., SPAIN_2024
        SILVER/SPAIN  

        """
    
        return f"{stage.upper()}/{country.upper()}"


    def metadata_object_name(self, country:str, stage:str) -> str:
        """Builds the S3 path: _metadata/LANDING/SPAIN.json"""
        
        key = self.dataset_key(country, stage) # ex (SPAIN, LANDING)
        
        return f"_metadata/{key}.json" # ex (_metadata/BRONZE/SPAIN)


    def load(self, country:str, stage: str ) -> dict:
        """Loads metadata JSON from MinIO. If missing or fails, returns empty dict."""
        
        METADATA_OBJECT = self.metadata_object_name(country, stage)
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

            logger.debug(f"Metadata loaded successfully for {METADATA_OBJECT}")
            return data
        except S3Error as e:
            if e.code in(
                "NoSuchKey",
                "NoSuchObject",
                "NoSuchBucket"
            ):
            # Silent fallback is intentional for first-run idempotency
               logger.info(f"No metadata found for {country.upper()}/{stage.upper()} ")
               return {}
            logger.error(f"Failed to load Metadata {METADATA_OBJECT}")
            raise
        finally:
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)

    def save(self, country:str, stage: str, metadata: dict):
        """Saves metadata JSON to MinIO with proper cleanup."""
        METADATA_OBJECT = self.metadata_object_name(country, stage)
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

    def compute_fingerprint(self, file_path: str) -> str:
        """Computes MD5 hash of file in chunks to handle large datasets efficiently."""
        hasher = hashlib.md5()
        with open(file_path, "rb") as f:
            while chunk := f.read(8192):
                hasher.update(chunk)
        return hasher.hexdigest()


    def is_processed(self, country:str, stage:str, dataset_id: str,  fingerprint: Optional[str]= None) -> bool:
        """Checks if a dataset with the matching hash has already been completed."""
        metadata = self.get_record(country, stage, dataset_id)
        if not metadata:
            return False
        
        return (metadata.get("status") == "processed" and metadata.get("hash") == fingerprint)


    def mark_processed(self, stage:str, dataset_id: str,  fingerprint: Optional[str]= None, metadata: Optional[Dict[str, Any]]=None):
        """
        Records a successful ingestion event.
        Updates metadata after a sucessful pipeline stage
        """
        country, year = dataset_id.split("_")

        records= self.load(country, stage)
        record= records.get(dataset_id, {})

        record.setdefault(
            "created_at", datetime.now(timezone.utc).isoformat()
        )
        record["updated_at"]= datetime.now(timezone.utc).isoformat()

        #Build new record
        record["dataset_id"]= dataset_id
        record["country"]= country.upper()
        record["year"]= year
        record["stage"]= stage.upper()
        record["status"]= "processed"
        record["fingerprint"]= fingerprint
        
        record["metadata"]= metadata or {}
        records[dataset_id]= record

        self.save(country, stage, records)
        logger.info(f"Marked processed: {dataset_id.upper()} as processed"
          f" for {stage.upper()}")


    def mark_failed(self, stage: str, dataset_id: str,  reason: str, metadata: Optional[Dict[str, Any]]=None):
        """Records a failure for debugging and visibility."""
        country,year= dataset_id.split("_")

        records= self.load(country, stage)
        record= records.get(dataset_id, {})
        record.setdefault(
            "created_at", datetime.now(timezone.utc).isoformat()
        )
        record["updated_at"]= datetime.now(timezone.utc).isoformat()


        record["dataset_id"]= dataset_id
        record["country"]= country
        record["year"]= year
        record["stage"]= stage
        record["status"]= "failed"
        record["reason"]= reason

        record["metadata"]= metadata or {}
        records[dataset_id]= record
        
        self.save(country, stage, records)
        logger.warning(f"Marked failed: {country.upper()} {dataset_id.upper()}")

    def get_record(self, country:str, stage: str, dataset_id: str) -> dict:
        """
        Retrieve metadata record for a dataset.

        Examples:
        
        dataset_id= SPAIN_2023
        
        looks inside
        _metadata/SILVER/SPAIN.json and returns:
        metadata["SPAIN_2023"]
        SPAIN_2024
        """

        metadata= self.load(country, stage)
        return metadata.get(
            dataset_id,
            {}
        )


    def list_all(self) -> list:
        """Returns a list of all metadata object names available."""
        return self.minio.list_object_names(
            bucket_name=MINIO_RAW_BUCKET,
            prefix="_metadata/"
        )