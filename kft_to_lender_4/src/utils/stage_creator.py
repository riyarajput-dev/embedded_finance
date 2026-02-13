import sys
from pathlib import Path
project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
from box import ConfigBox
from ..utils import aws_credentials
from .. import logger_ktl
from .. import config_ktl
from ..modules import session_ktl
from .. import snowflake_connection_obj

logger = logger_ktl
class StageCreator:
    def __init__(self, config: ConfigBox) -> None:
        self.config = config
        self.credentials = aws_credentials
        logger.info("[STAGE CREATION] Initializing stage creation process.")
       

    def create_stage(self, lender_name=None, bureau_type=None):
        # Use dynamic lender_name if provided, otherwise fall back to config
        if lender_name and bureau_type:
            S3_url = f"s3://{self.config.location.bucket_name}/kft-to-lender/{lender_name}_{bureau_type.lower()}/"
            stage_name = f"{lender_name}_{bureau_type.lower()}_stage"
        elif lender_name:
            S3_url = f"s3://{self.config.location.bucket_name}/kft-to-lender/{lender_name}/"
            stage_name = f"{lender_name}_stage"
        else:
            S3_url = f"s3://{self.config.location.bucket_name}/kft-to-lender/{self.config.location.lender_name}/"
            stage_name = self.config.location.stage_name
        
        logger.info(
            f"[STAGE CREATION] creating stage {stage_name} at {S3_url}..."
        )

        query = f"""
            CREATE OR REPLACE STAGE {stage_name}
            URL='{S3_url}'
            CREDENTIALS=(AWS_KEY_ID='{self.credentials.get("aws_key")}'
                        AWS_SECRET_KEY='{self.credentials.get("aws_secret")}')
        """

        try:
            session_ktl.sql(query).collect()   
            logger.info(
                f"[STAGE CREATION] stage {stage_name} created successfully."
            )
            return stage_name
        except Exception as e:
            logger.error(
                f"[STAGE CREATION] failed to create stage {stage_name}: {e}"
            )
            raise


