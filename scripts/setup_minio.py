# first time setup (admin user only)
from src.utils.logger import get_logger
from src.utils.minio_clients import MinIOClient
from src.utils.config import (
    MINIO_RAW_BUCKET,
    MINIO_BRONZE_BUCKET, 
    MINIO_SILVER_BUCKET,
    MINIO_GOLD_BUCKET
)

logger= get_logger(__name__)

def create_required_buckets(minio:MinIOClient):

    """
    create all  required project buckets if tey do not exists.
    Admin credentials  required
    """

    buckets= [
        MINIO_RAW_BUCKET,
        MINIO_BRONZE_BUCKET,
        MINIO_SILVER_BUCKET,
        MINIO_GOLD_BUCKET
    ]

    for bucket in buckets:
        try:
            minio.ensure_bucket(bucket)
            logger.info(f"Bucket ready: {bucket}")

        except Exception as e:
            logger.error(f"Failed creating bucket {bucket}: {e}")
            raise




def main():
    logger.info(f"Starting MinIO infrastructure setup")
    minio=MinIOClient()
    create_required_buckets(minio)
    logger.info("MinIO setup completed successfully")


if __name__ == "__main__":
    main()
