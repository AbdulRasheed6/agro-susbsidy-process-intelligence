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
from typing import Optional
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from src.utils.request_headers import build_headers
import time
import random

logger = get_logger(__name__)
metadata= MetadataManager()
MAX_RETRIES=3

def resolve_uk_download_url(country:str, url: str, year: str) -> str:
    """
    Dynamically resolve latest UK DEFRA excel file
    """
    
    logger.info(f"Resolving UK download  for {year}")

    time.sleep(random.uniform(40, 500))
    response= requests.get(
        url, 
        headers=build_headers(country),
        timeout=40,
        allow_redirects=True,
        verify=True
    )
    response.raise_for_status()
    soup=BeautifulSoup(response.text, "html.parser")

    for link in soup.find_all("a", href=True):
        href= link["href"]
        href_lower= href.lower()

        if ( year in href and ( ".xlsx" in href_lower or ".xls" in href_lower)):
            return urljoin(url, href)
    raise ValueError(f" NO UK excel found for {year}")



def download_file(country:str, url: str, output_path: str) -> bool:
    """
    Downloads a file from a URL using streaming to save memory
    """
    logger.info(f"Starting download from: {url}")

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, stream=True, timeout=100, headers=build_headers(country), verify=True)
            response.raise_for_status()
    

            with open(output_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

            logger.info("Download complete")
            return True

        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt + 1}/{MAX_RETRIES}"
                            f"failed  for {country} {url}: {e}")
            
            # exponential backoff before retrying
            if attempt < MAX_RETRIES - 1:
                sleep_time= 2 ** attempt
                logger.info(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
    logger.error(f"All retries failed for {country} {url}: {e}")
    return False

def get_file_extension(file_path:str) -> str:
    return Path(file_path).suffix.lower()

def extract_zip_if_needed(file_path: str, extract_dir: str) -> Optional[str]:
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
    base_path= os.path.splitext(file_path)[0]
    csv_path= f"{base_path}.csv"
    logger.info(f"converting excel to csv")

    df =pd.read_excel(file_path)
    df.to_csv(csv_path, index=False, encoding='utf-8')
    return csv_path

def find_files(directory: str):
    """
    Recursively finds all supported data files in a directory
    """
    supported_extensions=(".csv", ".xlsx", ".xls", ".parquet", ".json", ".txt")

    data_files = []

    logger.info(f"Scanning extracted directory: {directory}")

    for root, _, files in os.walk(directory):
        logger.info(f"Current root: {root}")

        logger.info(f"Files discovered: {files}")

        for file in files:
            suffixes= [s.lower() for s in Path(file).suffixes]

            if any(ext in suffixes for ext in supported_extensions):
                full_path= os.path.join(root, file)

                data_files.append(full_path)
                logger.info(f"Found data file: {full_path}")

         
    return data_files

def process_dataset(country:str, year: str, dataset_config: dict, country_config:dict):

    minio= MinIOClient()

    url= dataset_config["url"]
    network_config= country_config.get("network", {})
    
    # rate limiting
    if network_config.get("stagger_download"):
        stagger_time= random.uniform(
            network_config.get("min_delay", 2),
            network_config.get("max_delay", 7)
        )

        logger.info(f"Staggering {country} {year} download for {stagger_time:.2f} seconds")
        time.sleep(stagger_time)

    try:
       
        if country.upper() == "UK":
            url= resolve_uk_download_url(
                country,
                url, 
                year 
                
            )
        with tempfile.TemporaryDirectory() as temp_dir:

            remote_filename= url.split("/")[-1]
            file_ext=os.path.splitext(remote_filename)[1]

            if not file_ext:
                file_ext= (
                    ".xlsx" if country.upper()== "UK" else ".csv"
                )

            download_path= os.path.join(temp_dir, f"{country}_{year}{file_ext}")

            #1. Download
            
            
            success= download_file(country, url, download_path)
            
            if not success:
                return
            
            # 2. hash check
            file_hash= metadata.compute_hash(download_path)
            
            landing_id= f"{year}_LANDING"
            if metadata.is_processed(country, landing_id, file_hash):
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
                    converted= convert_excel_to_csv(file)
                    processed_files.append(converted)
                else:
                   processed_files.append(file)
            
            #5. Upload
            uploaded_count= 0
            
            
            

            for file in processed_files:
                object_name = f"{country}/{landing_id}/{Path(file).name}"
                try:
                    minio.upload_file(
                        bucket_name=MINIO_RAW_BUCKET,
                        object_name=object_name,
                        file_path=file
                    )
                    uploaded_count += 1
                    logger.info(f"Uploaded {object_name}")
                except Exception as e:
                    
                    logger.error(f"Failed to upload {object_name}: {e}")
            
            #verify uploads
            expected_files= len(processed_files)
            if uploaded_count !=expected_files:
                raise RuntimeError(
                    f"Upload incomplete for "
                    f"{country} {year}:"
                    f"{uploaded_count}/"
                    f"{expected_files} uploaded"
                )
            # ONLY mark processed if everything went up
            
            metadata.mark_processed(
                    country=country,
                    dataset_id=landing_id,
                    file_hash=file_hash,
                    files_uploaded=uploaded_count
                )
    except Exception as e :
        landing_id= f"{year}_LANDING"
        metadata.mark_failed(country=country, dataset_id=landing_id, reason=str(e))
        logger.error(f"{country} {year} failed: {e}")
        
def main():
    
    logger.info(
        "Starting download + staging pipeline"
    )
    """
    High - Level Flow
    External Source --> Download --> Validate --> Extract --> Normalise --> Upload to Raw Bucket --> Update Metadata
    """
    
    tasks= []

    for country, config in DATA_CATALOG.items():

        # skip scrapper sources now
        if (
            config.get("engine") == "scraper"
            and country.upper() != "UK"

        ):
            logger.warning(f"Skipping scraper source: {country}")
            continue

        for year, dataset_config in config["datasets"].items():
        
            tasks.append((country, year, dataset_config, config))
            logger.info(f"Total datasets queued: {len(tasks)}")


    #Parallel download  + Staging

    
    with ThreadPoolExecutor(max_workers=8) as executor:

        futures=[
            executor.submit(process_dataset, country, year, dataset_config, country_config)
            for country, year, dataset_config, country_config in tasks
        ]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:

                logger.error(f"Task failed: {e}")

if __name__ == "__main__":
    main()       
