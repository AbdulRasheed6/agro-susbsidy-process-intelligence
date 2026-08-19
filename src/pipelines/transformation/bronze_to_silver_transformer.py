
from src.pipelines.transformation.registry import DATASET_REGISTRY
from src.utils.config import  BRONZE_TO_SILVER
from src.pipelines.transformation.schemas import Silver_Schema, SILVER_TEXT_COLUMNS, SILVER_AMOUNT_COLUMNS
from src.utils.config import MINIO_BRONZE_BUCKET, MINIO_SILVER_BUCKET
from src.utils.logger import get_logger
from src.pipelines.validation.schema_validator import SchemaValidator, SchemaValidationError
from src.pipelines.validation.quality_validator import QualityValidator, QualityValidationError
from src.utils.spark_session import create_spark_session
from src.utils.metadata_manager import MetadataManager
from typing import  List
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

class BronzeToSilverTransformer:
    """
    Generic Bronze -> Silver transformer.
    Every transformation is driven entirely from the dataset configuration stored in registry.py

    Responsibilities

    * Apply dataset preprocessor
    * Rename bronze columns into canonical silver schema
    * Apply numeric cleaning strategies
    * Fill nmeric nulls
    * Fill text nulls
    * Add silver metadata columns 
    """

    def __init__(self):
        self.spark= create_spark_session("bronze_to_silver")
        self.metadata= MetadataManager()
        self.logger= get_logger(__name__)
        self.schema_validator= SchemaValidator(expected_schema=Silver_Schema)

    def transform_all(self):
        self.logger.info("Starting  Bronze -> Silver transformation")
        for dataset_id, dataset_config in DATASET_REGISTRY.items():
            self.transform_dataset(dataset_id)
        self.logger.info("Bronze -> Silver transformation completed")

    def _read_bronze(self, dataset_id:str) -> DataFrame:
        country, year= dataset_id.split("_")
        bronze_path = (f"s3a://{MINIO_BRONZE_BUCKET}/{country}/{year}_BRONZE/")
        self.logger.info(f"Reading data from database")
        return self.spark.read.parquet(bronze_path)

    def _fill_nulls(self, df:DataFrame) ->DataFrame:
        df=  df.fillna(
            "UNKNOWN",
            subset= SILVER_TEXT_COLUMNS
        )
        df= df.fillna(
            0.0,
            subset= SILVER_AMOUNT_COLUMNS
        )

        return df
    
    def _validate(self, df:DataFrame) -> None:
        self.schema_validator.validate(df) # performs every schema validation we have defined

        quality_validator= QualityValidator(
            required_columns=list(Silver_Schema.keys()), 
            amount_columns=SILVER_AMOUNT_COLUMNS
            )
        quality_validator.validate(df)
    
    def _create_silver_dataframe(self, bronze_df:DataFrame, dataset_config:dict) -> DataFrame:

        """
        Transform  a Bronze dataframe into the canonical silver dataframe
        """
        # dataset preprocessor
        preprocessor= dataset_config["preprocessor"]
        bronze_df= preprocessor(bronze_df)

        # build select expressions
        expressions= self._build_select_expressions(dataset_config)
        silver_df= bronze_df.select(*expressions)

        # fill null values

        silver_df= self._fill_nulls(silver_df)

        # transformation timestamp
        silver_df= self._add_metadata_columns(
            silver_df
        )

        return silver_df
    

    def _write_silver(self, df:DataFrame, dataset_id:str):
        country, year= dataset_id.split("_")
        silver_path= (f"s3a://{MINIO_SILVER_BUCKET}/{country}/{year}_SILVER")

        self.logger.info(f"Writing to silver bucket {silver_path}")

        df.write.mode("overwrite").parquet(silver_path)
        self.logger.info(f"Written to silver: {silver_path}")


    def _build_select_expressions(self, dataset_config:dict) -> List:
        """
        Dynamically build the select() expressions from the registry

        Returns
        [
          F.col(...).alias(...),
          cleaner(...).alias(...)
        ]
        """

        expressions= []

        
        # standard mapped columns
        mapping= dataset_config["columns"]
        for silver_column, details in mapping.items():
            source_column= details["source"]
            transform= details["transform"]
            expression= F.col(source_column)
            if transform is not None:
              expression= transform(source_column)
            expressions.append(
                expression.alias(silver_column)
                )

       
        return expressions


    def _add_metadata_columns(self, df:DataFrame) -> DataFrame:
        """
        Add ETL metadata columns. every silver dataset recieves tese columns automatically
        """
        df = (
            df.withColumn("silver_created_at", F.current_timestamp())
        )
        return df

    def transform_dataset(self, dataset_id:str) -> None:
        """
        Executes Bronze -> Silver transformation for a single dataset
        1. load dataset configuration 
        2. Read bronze metadata
        3. Skip if silver is already up-to-date
        4. Read from bronze 
        5. Transform columns
        6. Validate scemas
        7. Validate quality
        8. Write silver dataset
        9. Update silver metadata
        """
        self.logger.info(f"Starting Bronze -> Silver transformation ")

        dataset_config= DATASET_REGISTRY[dataset_id]

        silver_df=None
        part= dataset_id.split("_") #["SPAIN", "2023"]
        country, year= part[0], part[1]
        
        stage= "SILVER"
        
        

        # Read SILVER metadata
        #silver_record= self.metadata.get_record(country=country, stage=stage, dataset_id=dataset_id ) 
        #silver_hash= silver_record.get("hash")

        
        try:

            if self.metadata.is_processed(
            country=country,
            stage=stage,
            dataset_id=dataset_id):

               self.logger.info(f"{dataset_id} already transformed. Skipping")

               return

            #Read Bronze
            self.logger.info(f"Reading Bronze dataset for {dataset_id}")
            df= self._read_bronze(dataset_id)
            

            # Transform
            silver_df= self._create_silver_dataframe(df, dataset_config)
            self.logger.debug(f"Silver dataframe created for {dataset_id}")

            # Validate
            self.logger.info(f"Validating silver dataset for  {dataset_id}")
            self._validate(silver_df)


            # cache
            #silver_df.cache()
            #rows_written= silver_df.count()

            # Write to silver
            self.logger.debug(f"Writing to silver bucket for {dataset_id}")
            silver_df= silver_df.coalesce(4) 
            self._write_silver(silver_df, dataset_id)
            
            # Update metadata

            self.metadata.mark_processed(
                stage=stage,
                dataset_id= dataset_id,
                fingerprint=None,
                metadata= {}
            )
            self.logger.info(f"Completed Bronze -> Silver transformation for {dataset_id}")
         
        except SchemaValidationError as e:
            self.logger.error(f"Schema Validation failed: {e}")
            raise

        except QualityValidationError as e:
            self.logger.error(f"Qality Validation failed :{e}")
            self.metadata.mark_failed(
                stage=stage,
                dataset_id=dataset_id,
                reason= str(e),
                metadata= {
                    "reason": str(e)
                }
            )
            raise
        
        except Exception as e:
            self.logger.exception(
                f"Unexpected failure: {e}"
            )
            self.metadata.mark_failed(
                stage=stage,
                dataset_id=dataset_id,
                
                reason= str(e),
                metadata= {
                    "error_type": type(e).__name__
                }
            )
            raise
        finally:
            if silver_df is not None:
                silver_df.unpersist()
        

            


def main():
    transformer=BronzeToSilverTransformer()
    try:
       transformer.transform_all()
    finally:
        transformer.spark.stop()
    

if __name__=="__main__":
    main()