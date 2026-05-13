import os
import requests
import zipfile
import tempfile
from pathlib import Path

from src.utils.logger import get_logger
from src.utils.config import MINIO_RAW_BUCKET
from src.utils.minio_clients import MinIOClient
from configs.manifest import DATA_CATALOG
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.utils.metadata_manager import MetadataManager
import pandas as pd

logger = get_logger(__name__)
metadata= MetadataManager()

def download_file(url: str, output_path: str) -> bool:
    """
    Downloads a file from a URL using streaming to save memory
    """
    logger.info(f"Starting download from: {url}")

    try:
        response = requests.get(url, stream=True, timeout=1000)
        response.raise_for_status()

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        logger.info("Download complete")
        return True

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download file: {e}")
        return False

def get_file_extension(file_path:str) -> str:
    return Path(file_path).suffix.lower()

def extract_zip_if_needed(file_path: str, extract_dir: str) -> str | None:
    """
    Extracts a zip file archive to a specific directory
    """
    

    if file_path.endswith(".zip"):
        logger.info(f"Zip file detected")
        

        try:
            with zipfile.ZipFile(file_path, "r") as zip_ref:
                zip_ref.extractall(extract_dir)

            logger.info("Extraction completed")
            return extract_dir

        except Exception as e:
            logger.error(f"Extraction failed : {e}")
            return None
    
    return file_path

def convert_excel_to_csv(file_path:str):
    """
    convert excel file to csv since spark does not support excel files
    """

    csv_path= file_path.replace(".xlsx", ".csv").replace(".xls", ".csv")

    df =pd.read_excel(file_path)
    df.to_csv(csv_path, index=False)
    return csv_path

def find_files(directory: str):
    """
    Recursively finds all supported data files in a directory
    """
    supported_extensions=[".csv", ".xlsx", ".xls", ".parquet", ".json"]

    data_files = []

    for root, _, files in os.walk(directory):
        for file in files:
            if file.lower().endswith(supported_extensions):
                data_files.append(os.path.join(root, file))

    return data_files

def process_dataset(country:str, year: str, url:str):
    minio= MinIOClient()
    with tempfile.TemporaryDirectory() as temp_dir:
        download_path= os.path.join(temp_dir, f"{country}_{year}")

        #1. Download
        success= download_file(url, download_path)
        if not success:
            return
        # 2. hash check
        file_hash= metadata.compute_hash(download_path)
        
        if metadata.is_processed(country, year, file_hash):
            logger.info(f"Skipping (already processed)")
            return
        
        
        #3. Extract if needed
        result_path= extract_zip_if_needed(download_path, temp_dir)
    
        if not result_path:
            return
        
        # 4. Find files
        if os.path.isdir(result_path):
            files_path= find_files(result_path)
    
        else:
            files_path= [result_path]

        if not files_path:
            logger.warning(f" No usable files for {country} {year}")
            return
        
        #4.5 Normalise files
        processed_files= []
        
        for file in files_path:
            if file.lower().endswith((".xlsx", ".xls")):
                new_file_path= convert_excel_to_csv(file)
                processed_files.append(new_file_path)
            else:
               processed_files.append(file)
        
        #5. Upload
        uploaded_count= 0
        
        
        
        for f in processed_files:
            object_name= f"{country}/{year}/{Path(f).name}"

            try:
                minio.upload_file(
                    bucket_name=MINIO_RAW_BUCKET,
                    object_name=object_name,
                    file_path=f
                )
                uploaded_count +=1
                logger.info(f"Uploaded {object_name}")
                metadata.mark_processed(
                    country=country,
                    year=year,
                    file_hash=file_hash,
                    file_uploaded=uploaded_count
                    )
                

            except Exception as e:
                metadata.mark_failed(country, year, str(e))
                logger.error(f"{country} {year} failed: {e}")

        


def main():
    
    tasks= []

    for country, config in DATA_CATALOG.items():

        # skip scrapper sources now
        if config.get("engine") == "scraper":
            logger.warning(f"Skipping scraper source: {country}")
            continue

        for year, url in config["datasets"].items():
            tasks.append((country, year, url))
            logger.info(f"Total datasets queued: {len(tasks)}")


    #Parallel download  + Staging

    
    with ThreadPoolExecutor(max_workers=4) as executor:

        futures=[
            executor.submit(process_dataset, country, year, url)
            for country, year, url in tasks
        ]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:

                logger.error(f"Task failed: {e}")

if __name__ == "__main__":
    main()       
