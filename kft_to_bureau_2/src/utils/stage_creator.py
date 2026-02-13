import sys
from pathlib import Path
from box import ConfigBox
from ..utils import aws_credentials
from .. import logger_ktb
from .. import config_ktb
from ..modules import session_ktb, read_format
from .. import snowflake_connection_obj


class StageCreator:
    def __init__(self, config_ktb: ConfigBox) -> None:
        self.config = config_ktb
        self.credentials = aws_credentials
        logger_ktb.info("[STAGE CREATION] Initializing stage creation process.")
       

    def create_stage(self):
        S3_url = f"s3://{self.config.location.bucket_name}/{self.config.location.folder_name}/{self.config.location.fetch_object_name}/"
        
        logger_ktb.info(
            f"[STAGE CREATION] creating stage {self.config.location.stage_name} at {S3_url}..."
        )

        query = f"""
            CREATE OR REPLACE STAGE {self.config.location.stage_name}
            URL='{S3_url}'
            CREDENTIALS=(AWS_KEY_ID='{self.credentials.get("aws_key")}'
                        AWS_SECRET_KEY='{self.credentials.get("aws_secret")}')
        """

        try:
            session_ktb.sql(query).collect()   
            logger_ktb.info(
                f"[STAGE CREATION] stage {self.config.location.stage_name} created successfully."
            )
        except Exception as e:
            logger_ktb.error(
                f"[STAGE CREATION] failed to create stage {self.config.location.stage_name}: {e}"
            )
            raise


