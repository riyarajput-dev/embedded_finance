from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame
import snowflake.snowpark.functions as F
from .utils.s3_logger import setup_s3_logger
import logging
import os
from cryptography.hazmat.primitives import serialization
from box import ConfigBox
import yaml
from dotenv import load_dotenv
from datetime import date
import io

# Load environment variables first to ensure credentials are available
load_dotenv()

with open('riya_private_key.pem', 'rb') as key:
    private_key = serialization.load_pem_private_key(
        key.read(),
        password=None  
    )

# Initialize the S3 Logger
try:
    logger_ktb = setup_s3_logger()
    logger_ktb.info("S3 Logger initialized.")
except Exception as e:
    # Fallback to standard logging if S3 fails
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("logs")
    logger_ktb.error(f"Failed to initialize S3 Logger: {e}")

def load_config(config_path="kft_to_bureau_2/config/config.yaml"):
    with open(config_path, 'r') as f:
        config_dict = yaml.safe_load(f)
    return ConfigBox(config_dict, default_box=True)

try:
    config_ktb = load_config()
except Exception as e:
    logger_ktb.warning(f"Could not load config: {e}")
    config_ktb = None  

snowflake_creds = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "private_key": private_key
}




def snowflake_connection_obj() -> Session:
    logger_ktb.info("creating snowflake session.")
    connection_parameters = snowflake_creds
    session = Session.builder.configs(connection_parameters).create()

    logger_ktb.info("session created successfully.")
    return session

try:
    session_ktb = snowflake_connection_obj()
except Exception as e:
    logger_ktb.info(f"Snowflake connection failed: {e}")


def save_table(
    df: DataFrame,
    table_name: str,
    batch:int,
    mode: str,
    table_type: str = ""
 
):
    df = df.with_column("SENT_TO_EXPERIAN", F.lit(False))
    df = df.with_column("SCRUB_DATE", F.to_date(F.lit(f"{date.today()}")))
    df = df.with_column("SCRUB_BATCH_NO", F.lit(f"batch_{batch}")) 
        
    
    column_order = "NAME"
    df.write.save_as_table(
        table_name, mode=mode, table_type=table_type, column_order=column_order
    )
    
    
    
    
  