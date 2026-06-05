from minio import Minio
from minio.error import S3Error
from src.utils.logger import get_logger
import json
import os

from src.utils.config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_SECURE
)
logger= get_logger(__name__)


class MinIOClient:
    def __init__(self):
        self.client= Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key= MINIO_SECRET_KEY,
            secure= MINIO_SECURE
        )
    """ADMIN ONLY OPERATIONS"""
    # Bucket operations
    def ensure_bucket(self, bucket_name:str):

        """
        Ensure a bucket exists. create it if missing
        """

        try:
            if not self.client.bucket_exists(bucket_name):
               logger.info(f"Bucket '{bucket_name} does not exist. creating")
               self.client.make_bucket(bucket_name)
            else:
                logger.debug(f"Bucket '{bucket_name} already exists.")

        except S3Error as e:
            logger.error(f"Error ensuring bucket '{bucket_name}': {e}")
            raise

    # DATA OPERATIONS    

    def upload_file(self, bucket_name: str, object_name: str, file_path: str):
        """
        Upload local file to MinIO safely.
        Uses fput_object for robust multipart uploads.
        """

        try:

            # Ensure file fully flushed to disk
            os.sync()

            # File diagnostics
            file_size = os.path.getsize(file_path)

            logger.info(f"Uploading file: {file_path} "
                f"({file_size / (1024 * 1024):.2f} MB)"
            )

            self.client.fput_object(bucket_name=bucket_name, object_name=object_name,file_path=file_path)

            logger.info(f"Uploaded {file_path} "
                f"to {bucket_name}/{object_name}"
            )

        except Exception as e:

            logger.error(f"Upload failed: {e}")
            raise

    def list_objects(self, bucket_name:str, prefix:str=None, recursive:bool=True):

        """
        Lists objects in a bucket , optionally filtered by a prefix
        """

        try:
            return self.client.list_objects(
                bucket_name,
                prefix=prefix,
                recursive=recursive
            )
        except S3Error as e:
            logger.error(f"Error listing objects in bucket '{bucket_name}': {e}")
            raise



    def download_file(self, bucket_name:str, object_name:str, file_path:str):

        """
        Downloads an object from MinIO to the local filesystem.
        """
        try: 
            self.client.fget_object(bucket_name, object_name, file_path)
            logger.info(f"Successfully downloaded '{object_name}' to '{file_path}'")

        except S3Error as e:
            logger.error(f"Failed to download'{object_name}' from MinIO: {e}")
            raise

    def list_object_names(self, bucket_name:str, prefix : str=None):
        """
        List names of objects inside a bucket
        """

        try:
            
            objects=self.client.list_objects(
                bucket_name, 
                prefix=prefix,
                recursive=True)

            return [obj.object_name for obj in objects]
        
        except S3Error as e:
            logger.error(f"Error listing objects : {e}")
            return []


    def delete_object(self, bucket_name:str, object_name:str):

        """
        Delete an object from a bucket
        """

        try:
            
            self.client.remove_object(bucket_name, object_name)
            logger.warning(f"Deleting object  '{object_name}' from bucket '{bucket_name}'")

        
        except S3Error as e:
            logger.error(f"Error deleting object: {e}")
            raise


    