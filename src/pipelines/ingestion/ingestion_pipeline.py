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

    if extension == ".csv":
        return (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .option("delimeter", delimiter)
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
    lowercase + replace spacces with underscore.
    """

    for col_name in df.columns:
        new_name = (
            col_name.strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace("/", "_")
        )

        df=  df.withColumnRenamed(col_name, new_name)
    return df
    


# Enrich the dataframe

def enrich(df, country:str, year:str):

    return (
        df
        .withColumn("source_country", F.lit(country))
        .withColumn("source_year", F.lit(year))
        .withColumn("ingested_at", F.current_timestamp())
    )




# Process one dataset
def process_dataset(spark, country:str, year:str, layer:str, config:dict):

    """
    layer: BRONZE, SILVER
    raw -> bronze
    Idempotent by checking bronze completion state
    """

    metadata= MetadataManager()
    minio= MinIOClient()

    key= f"{country}_{year}"

    record= metadata.get_record(country, f"{year}_{layer}")

    if record.get("status") == "completed":
        logger.info(f"Skipping already processed dataset")
        return
    
    raw_prefix= f"{country}/{year}"

    try:
        objects= minio.list_object_names(
             bucket_name=MINIO_RAW_BUCKET,
            prefix=raw_prefix)
        files= [
            obj for obj in objects
            if obj.startswith(raw_prefix)
        ]

        if not files:
            logger.warning(f"No raw files available for {country} {year}")
            return

        logger.info(f"{country} {year}: {len(files)} raw file(s) found")


        frames= []

        for obj in files:
            ext= detect_extensions(obj)
            s3_path= f"s3a://{MINIO_RAW_BUCKET}/{obj}"

            df= read_with_spark(
                spark=spark,
                s3_path=s3_path,
                extension=ext,

                delimeter= config.get("delimiter", ",")
            )

            df =clean_columns(df)
            frames.append(df)

        # incase we are pulling data from multiple sources

        #union all the files
        if not frames:
            logger.warning(f"No valid dataframe create for {country} {year}")
            return
        
        df_final=frames[0]

        for df_part in frames[1:]:
            df_final= df_final.unionByName(
                df_part, 
                allowMissingColumns=True
            )


        # enrich
        df_final=enrich(df_final, country, year)

        bronze_path= f"s3a://{MINIO_BRONZE_BUCKET}/{country}/{year}"

        (
            df_final.write
            .mode("overwrite")
            .parquet(bronze_path)
        )

        logger.info(f"Wrote to bronze: {bronze_path}")

        metadata.mark_processed(
            country=country,
            year=f"{year}_BRONZE",
            file_hash="spark_write_complete",
            files_uploaded= 1
        )

    except Exception as e:
        logger.error(f"Bronze ingestion failed for {country} {year}: {e}")
        metadata.mark_failed(
            country=country,
            year= f"{year}_BRONZE",
            reason= str(e)
        )


def main():
    logger.info("Starting raw -> bronze ingestion pipeline")

    spark=create_spark_session("bronze_ingestion")

    try:
        for country, config in DATA_CATALOG.items():
            if config.get("engine") == "scraper":
                logger.warning(f"Skipping scraper source: {country}")
                continue
            for year in config["datasets"].keys():
                process_dataset(
                    spark=spark,
                    country=country,
                    year=year,
                    layer="BRONZE",
                    config=config
                )

        logger.info("Bronze ingestion completed")

    finally:
        spark.stop()
        logger.info("Spark session stopped")




if __name__== "__main__":
    main()





