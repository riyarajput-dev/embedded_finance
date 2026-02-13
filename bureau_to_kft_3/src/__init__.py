import logging
import os
import sys
import warnings
from datetime import date
from pathlib import Path
from dotenv import load_dotenv
from snowflake.snowpark import Session
import snowflake.snowpark.functions as F
from pathlib import Path
from cryptography.hazmat.primitives import serialization
import yaml
from box import ConfigBox
warnings.filterwarnings("ignore")

root = Path().cwd() 
config_path = "bureau_to_kft_3/config/config.yaml"

load_dotenv()
scrub_dt = date.today().__str__()



logging.basicConfig(
    filename="bureau_to_kft_3/logs/experian_logs.log",
    level=logging.INFO,
    format="%(asctime)s: %(levelname)s: %(module)s: %(message)s",
    # handlers=[logging.StreamHandler(sys.stdout)],
)
logger_btk = logging.getLogger("experian")
logging.getLogger("snowflake.connector.cursor").disabled = True
logger_btk.info(f"{'='*80}")
logger_btk.info(f"Loading files from {root}...")
logger_btk.info(f"Loading configurations from {config_path}...")
logger_btk.info(f"creating data based on scrub date {scrub_dt}")


scrub_dt = date.today().__str__()  # used for created_at field

private_key_path = "riya_private_key.pem"  # Replace with your private key path

with open(private_key_path, "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None  # Add the password here if your key is password protected
    )

# credentials
aws_credentials = {
    "aws_key": os.getenv("AWS_KEY"),
    "aws_secret": os.getenv("AWS_SECRET_KEY"),
    "aws_region":os.getenv("AWS_REGION")
}

snowflake_creds = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "private_key":private_key
}



def snowflake_connection_obj(database: str, schema: str) -> Session:
    logger_btk.info("creating snowflake session.")
    connection_parameters = snowflake_creds
    session = Session.builder.configs(connection_parameters).create()
    # tableName = "information_schema.packages"
    # dataframe = session.table(tableName).filter(F.col("language") == "python")
    # session.sql(f"use database {database}").collect()
    # session.sql(f"use schema {schema}").collect()
    # logger_btk.info(f"using database {database} and schema {schema}")
    logger_btk.info("session created successfully.")
    return session



def read_yaml(path: Path):
    with open(path, "r") as j:
        content = yaml.safe_load(j)
    config = ConfigBox(content)
    return config

config_btk = read_yaml(config_path)
logger_btk.info(f"loaded {config_btk.name} config.")