from box import ConfigBox
from ....src import aws_credentials, logger_btk
from ...utils.utilities import generate_ascii_art
from ...utils.utilities import S3_loc
from ...etl.pipeline import session_btk
logger = logger_btk

class StageCreator:
    def __init__(self, config_btk: ConfigBox) -> None:
        self.config = config_btk
        self.credentials = aws_credentials
        logger.info("[STAGE CREATION] Initializing stage creation process.")
        generate_ascii_art("Stage Creation")

    def create_stage(self, folder_name: str):
        S3_url = S3_loc(
            self.config.location.batch,
            folder_name,
            self.config.location.object_name,
            self.config.location.bucket_name,
        )
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
        state = session_btk.sql(query)
        logger.info(
            f"[STAGE CREATION] stage {self.config.location.stage_name} created successfully."
        )
        logger.info(f"\n{'-'*100}")
        return state.show()
