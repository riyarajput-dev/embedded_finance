import sys
from pathlib import Path
project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
from box import ConfigBox
from src.utils import aws_credentials
from src import logger_ptk
from src import config_ptk
from src.modules import session_ptk, read_format
from src import snowflake_connection_obj


class StageCreator:
    def __init__(self, config_ptk: ConfigBox) -> None:
        self.config_ptk = config_ptk
        self.credentials = aws_credentials
        logger_ptk.info("[STAGE CREATION] Initializing stage creation process.")
       

    def create_stage(self):
        S3_url = f"s3://{self.config_ptk.location.bucket_name}/lender-to-kft/{self.config_ptk.location.lender_name}"
        
        logger_ptk.info(
            f"[STAGE CREATION] creating stage {self.config_ptk.location.stage_name} at {S3_url}..."
        )

        query = f"""
            CREATE OR REPLACE STAGE {self.config_ptk.location.stage_name}
            URL='{S3_url}'
            CREDENTIALS=(AWS_KEY_ID='{self.credentials.get("aws_key")}'
                        AWS_SECRET_KEY='{self.credentials.get("aws_secret")}')
        """

        try:
            session_ptk.sql(query).collect()   
            logger_ptk.info(
                f"[STAGE CREATION] stage {self.config_ptk.location.stage_name} created successfully."
            )
        except Exception as e:
            logger_ptk.error(
                f"[STAGE CREATION] failed to create stage {self.config_ptk.location.stage_name}: {e}"
            )
            raise


