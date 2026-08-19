from pathlib import Path
from pyspark.sql import functions as F
from src.utils.logger import get_logger
from src.utils.spark_session import create_spark_session
from src.utils.minio_clients import MinIOClient
from src.utils.metadata_manager import MetadataManager
from src.utils.config import (
    MINIO_RAW_BUCKET,
    MINIO_BRONZE_BUCKET
)

from configs.manifest import DATA_CATALOG

logger= get_logger(__name__)

SUPPORTED_EXTENSIOS= (
    ".csv",
    ".txt",
    ".json",
    ".parquet"
)

# File type heplers

def detect_extensions(file_name:str) ->str:
    
    return Path(file_name).suffix.lower()
    


# Read file wit spark
def  read_with_spark(spark, s3_path: str, extension: str, delimiter: str = ","):

    """
    Read supported file types from MinIO using spark
    NOTE: we won't account for excel extension , since every excel file must have been converted to csv
    """

    logger.info(f"Reading {s3_path}")

    if extension in [".csv", ".txt"]:
        return (
            spark.read
            .option("header", True)
            .option("delimiter", delimiter)
            .option("encoding", "ISO-8859-1")
            .option("multiLine", False)
            .option("mode", "PERMISSIVE")
            .option("ignoreLeadingWhiteSpace", True)
            .option("ignoreTrailingWhiteSpace", True)
            .option("maxColumns", 10000)
            .csv(s3_path)
        )
    

    elif extension ==".json":
        return (
            spark.read.option("multiline", True).json(s3_path)

        )
    
    elif extension == ".parquet":
        return spark.read.parquet(s3_path)
    
    else: 
        raise ValueError(f"Unsupported file type: {extension}")




# Standardise column names

def clean_columns(df):

    """
    lowercase + replace spaces with underscore.
    Tis function optimises the spark operation in order to prevent massive operation blow out siince spark operates on lazy  implementation
    Spark sees  Read Csv -> single projection instead of creating several new dataframe object for every loop 
    """

    expressions= [
        F.col(f"`{c}`").alias(
           c.strip()
            .lower()
            .replace(" ", "_" )
            .replace("-", "_")
            .replace("/", "_")
        )
        for c in df.columns
    ]

    
    return df.select(*expressions)
    


# Enrich the dataframe

def enrich(df, country:str, year:str):

    return (
        df
        .withColumn("source_country", F.lit(country))
        .withColumn("source_year", F.lit(year))
        .withColumn("ingested_at", F.current_timestamp())
    )




# Process one dataset
def process_dataset(spark, country:str, year:str, layer:str, dataset_config:dict):

    """
    layer: BRONZE
    raw -> bronze
    Idempotent by checking bronze completion state
    BRONZE/SPAIN.json
    SPAIN_2023
    """

    metadata= MetadataManager()
    minio= MinIOClient()

    # Bronze Dataset ID
    bronze_dataset_id= f"{year}_{layer}"

    # Get the Bronze Dataset ID record
    record= (
        metadata.get_record(country, bronze_dataset_id) or {}
    )
    # if it has been completed before skip 
    if record.get("status") == "completed":
        logger.info(f"Skipping already processed dataset:" f"{country}_{bronze_dataset_id}")
        return
    
    #Locate Raw files
    landing_id= f"{year}_LANDING"
    raw_prefix= f"{country}/{landing_dataset_id}"

    try:
        #List objects in MinIO
        objects= minio.list_object_names(
             bucket_name=MINIO_RAW_BUCKET,
            prefix=raw_prefix)
        files= []
        
        # Filter files that only comes from {year}_{Landing}
        for obj in objects:
            ext= detect_extensions(obj)

            if ext in SUPPORTED_EXTENSIOS:
                files.append(obj)
    

        if not files:
            logger.warning(f"No raw files available for {country} {year}")
            return

        logger.info(f"{country} {year}: {len(files)} raw file(s) found")


        combined_df=None

        for obj in files:
            logger.info(f"Processing object: {obj}")
            ext= detect_extensions(obj)

            #Build s3 path
            s3_path= f"s3a://{MINIO_RAW_BUCKET}/{obj}"
            df= read_with_spark(spark, s3_path, ext, dataset_config.get("delimiter", ";"))

            df= clean_columns(df)

            # Enrich the data with columns metadata
            df= enrich(df, country, year)

            # Combone files
            if combined_df is None:
                combined_df= df
            else:
                combined_df= combined_df.unionByName(df, allowMissingColumns=True)


        # Write to Bronze
        if combined_df is not None:

            bronze_path= (
                f"s3a://{MINIO_BRONZE_BUCKET}/"
                f"{country}/{bronze_dataset_id}/"
            )
            logger.info(f"Writting to bronze: {bronze_path}")
            (
                combined_df.write
                .mode("overwrite")
                .parquet(bronze_path)
            )

            logger.info(f"Written to bronze: {bronze_path}")
            
            #Mark Success
            metadata.mark_processed(
                stage= layer,
                dataset_id=f"{country.upper()}_{year}",
                
                fingerprint="spark_write_complete",
                metadata= {
                    "files_uploaded":len(files)
                }
            )

    #Failure handling
    except Exception as e:
        logger.error(f"Bronze ingestion failed for {country} {year}: {e}")
        metadata.mark_failed(
            stage=layer,
            dataset_id= f"{country.upper()}_{year}",
            
            fingerprint="spark_write_failed",
            reason= str(e)
            metadata= {
                "reason": str(e)
            }
        )


def main():


    logger.info("Starting raw -> bronze ingestion pipeline")

    spark=create_spark_session("bronze_ingestion")
    """
    High - Level Flow
    Catalog --> Skip(France and Dutch) --> Raw Bucket --> process_dataset() --> spark Reads Raw Files --> Clean Columns --> Add Metadata Columns --> Union Files --> Write to Bronze parquet --> Update Metadata
    """

    try:
        for country, config in DATA_CATALOG.items():
            if config.get("engine") == "scraper" and country.upper() != "UK":
                logger.warning(f"Skipping scraper source: {country}")
                continue
            for year, dataset_config in config["datasets"].items():
                process_dataset(
                    spark=spark,
                    country=country,
                    year=year,
                    layer="BRONZE",
                    dataset_config=dataset_config
                )

        logger.info("Bronze ingestion completed")

    finally:
        spark.stop()
        logger.info("Spark session stopped")




if __name__== "__main__":
    main()





