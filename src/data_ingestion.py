## Code to ingest data from GCP bucket and save it to local raw data directory
import os                                         ## os module to handle file paths and directories
import pandas as pd                               ## pandas to handle CSV files
from google.cloud import storage                  ## GCP storage client to interact with GCP buckets
from src.logger import get_logger                 ## custom logger for logging info and errors
from src.custom_exception import CustomException  ## custom exception for error handling
from config.paths_config import *                 ## import file paths from config
from utils.common_functions import read_yaml      ## function to read YAML config files

logger = get_logger(__name__)                     ## initialize logger for this module

class DataIngestion:                              ## DataIngestion class to handle data ingestion process
    def __init__(self,config):                    ## constructor to initialize config and GCP bucket details
        self.config = config["data_ingestion"]    ## extract data_ingestion config section
        self.bucket_name = self.config["bucket_name"]  ## GCP bucket name
        self.file_names = self.config["bucket_file_names"]   ## list of file names to download

        os.makedirs(RAW_DIR,exist_ok=True)        ## create raw data directory if it doesn't exist

        logger.info("Data Ingestion Started....")  ## log start of data ingestion

    def download_csv_from_gcp(self):              ## method to download CSV files from GCP bucket
        try:

            client  = storage.Client()            ## initialize GCP storage client
            bucket = client.bucket(self.bucket_name)  ## get the specified bucket

            for file_name in self.file_names:
                file_path = os.path.join(RAW_DIR,file_name)

                if file_name=="animelist.csv":
                    blob = bucket.blob(file_name)
                    blob.download_to_filename(file_path)

                    data = pd.read_csv(file_path,nrows=5000000)
                    data.to_csv(file_path,index=False)
                    logger.info("Large file detected Only downloading 5M rows")
                else:
                    blob = bucket.blob(file_name)
                    blob.download_to_filename(file_path)

                    logger.info("Downloading Smaller Files ie anime and anime_with synopsis")
        
        except Exception as e:
            logger.error("Error while downloading data from GCP")
            raise CustomException("Failed to download data",e)
        
    def run(self):
        try:
            logger.info("Starting Data Ingestion Process....")
            self.download_csv_from_gcp()
            logger.info("Data Ingestion Completed...")
        except CustomException as ce:
            logger.error(f"CustomException : {str(ce)}")
        finally:
            logger.info("Data Ingestion DONE...")


if __name__=="__main__":
    data_ingestion = DataIngestion(read_yaml(CONFIG_PATH))
    data_ingestion.run()