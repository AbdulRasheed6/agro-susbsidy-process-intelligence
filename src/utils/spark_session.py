from pyspark.sql import SparkSession
from src.utils.logger import get_logger
import src.utils.config as config 


print(f"DEBUG: Access Key is {config.MINIO_ACCESS_KEY[:3]} + ***") 
print(f"DEBUG: ENDPOINT is {config.MINIO_ENDPOINT}")

# Get the container's hostname dynamically

logger = get_logger(__name__)

def create_spark_session(app_name: str) -> SparkSession:
    """
    Creates and returns a configured Spark session for MinIO.
    """
    logger.info(f"Creating Spark Session: {app_name}")
    
    # This pulls the necessary S3A connectors from Maven Central
    
    

    spark = (   
        SparkSession.builder
        .appName(app_name)
        .master("spark://spark-master:7077")
        .config("spark.driver.host", config.DRIVER_HOST)
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.port", config.SPARK_DRIVER_PORT)
        .config("spark.driver.blockManager.port", config.SPARK_BLOCK_MANAGER_PORT)
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.executor.instances", "1") # adjust based on resources
        .config("spark.executor.cores", "2")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        #.config("spark.executor.memory", "2g") # adjust based on resources
        #.config("spark.driver.memory", "2g") # adjust based on resources

         # hadoop S3A Configuration
      
        .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(config.MINIO_SECURE).lower())
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") # prevent class resolution
        .config("spark.sql.caseSensitive", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.cores.max", "2")
        .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
        .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
        .config("spark.hadoop.fs.s3a.fast.upload", "true") 
        .config("spark.sql.files.maxPartitionBytes", "64m") #64MB partitions
        .config("spark.network.timeout", "1200s")
        .config("spark.rpc.askTimeout", "600s")
        .config("spark.executor.heartbeatInterval", "120s")
        .config("spark.hadoop.fs.s3a.connection.timeout", "600000")
        .config("spark.hadoop.fs.s3a.paging.maximum", "1000")
        
        .getOrCreate()
    )
    # Suppress verbose logs
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created successfully")
    return spark