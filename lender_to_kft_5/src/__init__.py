import logging
import os 
from box import ConfigBox
from snowflake.snowpark import Session
import yaml
from dotenv import load_dotenv
load_dotenv()
from cryptography.hazmat.primitives import serialization
from .utils.s3_logger import setup_logger
with open('riya_private_key.pem', 'rb') as key:
    private_key = serialization.load_pem_private_key(
        key.read(),
        password=None  
    )
    

logging.basicConfig(level=logging.INFO)
logger_ltk = logging.getLogger("logs")


def load_config(config_path="config/config.yaml"):
    with open(config_path, 'r') as f:
        config_dict = yaml.safe_load(f)
    return ConfigBox(config_dict, default_box=True)

try:
    config_ltk = load_config()
except Exception as e:
    config_ltk = None  

# logger_ltk = setup_logger(config_ltk)


snowflake_creds = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "private_key": private_key
}

print(snowflake_creds)

def snowflake_connection_obj(database: str, schema: str) -> Session:
    logger_ltk.info("creating snowflake session.")
    connection_parameters = snowflake_creds
    session = Session.builder.configs(connection_parameters).create()
    # tableName = "information_schema.packages"
    # dataframe = session.table(tableName).filter(F.col("language") == "python")
    session.sql(f"use database {database}").collect()
    session.sql(f"use schema {schema}").collect()
    logger_ltk.info(f"using database {database} and schema {schema}")
    logger_ltk.info("session created successfully.")
    return session


