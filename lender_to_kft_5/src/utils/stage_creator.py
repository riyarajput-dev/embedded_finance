
from box import ConfigBox
import boto3
from typing import Dict, List, Iterator, Tuple
import yaml
from ...src import logger_ltk
from ..utils import aws_credentials
from ..modules import session_ltk 
from ...src import config_ltk
import re
from datetime import datetime
import pandas as pd

from io import StringIO
logger = logger_ltk
class StageCreator:
    def __init__(self, config_ltk: ConfigBox) -> None:
        self.config = config_ltk
        self.credentials = aws_credentials
        logger.info("[STAGE CREATION] Initializing stage creation process.")

    def create_stage(self):
        S3_url = f"s3://lenders-data-exchange/lender-to-kft/{self.config.location.lender_name}/"
        logger.info(
            f"[STAGE CREATION] creating stage {self.config.location.stage_name} at {S3_url}..."
        )
        query = """create or replace stage {} 
                url='{}' 
                CREDENTIALS = (AWS_KEY_ID='{}' AWS_SECRET_KEY='{}')""".format(
            self.config.location.stage_name,
            S3_url,
            self.credentials.get("aws_key"),
            self.credentials.get("aws_secret"),
        )
        state = session_ltk.sql(query)
        logger.info(
            f"[STAGE CREATION] stage {self.config.location.stage_name} created successfully."
        )
        logger.info(f"\n{'-'*100}")
        return state.show()
