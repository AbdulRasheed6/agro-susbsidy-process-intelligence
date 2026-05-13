import  json
import hashlib
import tempfile
from datetime import datetime, timezone
from  src.utils.logger import get_logger
from src.utils.minio_clients import MinIOClient
from src.utils.config import MINIO_RAW_BUCKET

logger= get_logger(__name__)




class MetadataManager:
    """
    metadata manager for idempotent ingestion

    Tracks:
    -processed datasets
    -file ase
    -timestamps
    -upload counts
    -status
    """


    def __init__(self):
        self.minio= MinIOClient()


    # build datasest key

    def dataset_key(self, country:str, year:str) -> str:
        """
        Standard dataset key
        ex: SPAIN_2024
        """

        return f"{country.upper()}_{year}"
    
        #Load Metadata

    def metadata_object_name(self, country:str, year:str) -> str:
        """
        Build matadata object path
        Example:
        _metadata/SPAIN_2024.json
        """
        key= self.dataset_key(country, year)
        return  f"_metadata/{key}.json"


    def load(self, country:str, year:str) -> dict:
        """
        load metadata JSON from MinIO, if missing, return empty dict

        """
        METADATA_OBJECT=self.metadata_object_name(country, year)
        try:
            with tempfile.NamedTemporaryFile() as tmp:
                self.minio.download_file(
                    bucket_name=MINIO_RAW_BUCKET,
                    object_name=METADATA_OBJECT,
                    file_path=tmp.name
                )
                with open(tmp.name, "r", encoding="utf-8") as f:
                    data=json.load(f)

                logger.debug("Metadat loaded sucessfully")
                return data
        except Exception:
            logger.info("No metadata file found. Starting fres")
            return {}



    # save metadata
    def save(self, country: str, year: str, metadata:dict):
        """"
        Save metadata JSON to MinIO
        """
        METADATA_OBJECT=self.metadata_object_name(country, year)
        
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".json",
            delete=False,
            encoding="utf-8"

        ) as tmp:
            json.dump(metadata, tmp, indent=2)
            temp_path= tmp.name

        

        self.minio.upload_file(
            bucket_name=MINIO_RAW_BUCKET,
            object_name=METADATA_OBJECT,
            file_path=temp_path
        )

        logger.debug("Metadata saved successfully")



    # hash file
    def compute_hash(self, file_path:str) -> str:
        """
        Compute MD5 hash of file
        """

        hasher= hashlib.md5()
        with open(file_path, "rb") as f:
            while chunk:= f.read(8192):
                hasher.update(chunk)

        return hasher.hexdigest()
    


    

    # Check  if already processed

    def is_processed(self, country:str, year:str, file_hash:str) -> bool:

        """
        Return True if same dataset with same hash is laready processed
         """
        

        metadata= self.load(country, year)
        

        if not metadata:
            return False
        
        return (metadata.get("status") == "completed" and  metadata.get("hash") == file_hash)
    


    # Mark success
    def mark_processed(self, country:str, year:str, file_hash:str, file_uploaded:int=1):

        """
        Mark dataset successfully proccessed
        """
        
        metadata= {
            "status": "completed",
            "hash": file_hash,
            "files_uploaded": file_uploaded,
            "uploaded_at": datetime.now(timezone.utc).isoformat()
        }

        self.save(country, year, metadata)
        logger.info(f"Marked processed: {country} {year}")


   # Mark failure
    def mark_failed(self, country:str, year:str, reason:str):

        """
        Record failed inestion
        """


        metadata= {
            "status": "failed",
            "reason": reason,
            "updated_at": datetime.now(timezone.utc).isoformat()

        }
        self.save(country, year, metadata)
        logger.warning(f"Marked failed :{country} {year}")



    # Get dataset record

    def get_record(self, country:str, year:str, layer=None) -> dict:

        """
        # dataset_id examples:
        2021
        2021_BRONZE
        2021_SILVER
        Return metadata record for dataset


        """

        

        return self.load(country, year)
    

    # Remove dataset record

    def delete_record(self, country:str, year:str):

        """
        Remove dataset metadata entry
        """
        METADATA_OBJECT=self.metadata_object_name(country, year)

        try:
            self.minio.delete_object(
                bucket_name=MINIO_RAW_BUCKET,
                object_name=METADATA_OBJECT                     
                ) 
            
            logger.info(f"Deleted metadata record: {METADATA_OBJECT}")

        except Exception as e:
            logger.error(f"Failed deleting metadata: {e}")

    # list all record

    def list_all(self) -> dict:
        """
        Return all metadata
        """
        objects= self.minio.list_object_names(
            bucket_name=MINIO_RAW_BUCKET,
            prefix="_metadata/"
        )
        return objects