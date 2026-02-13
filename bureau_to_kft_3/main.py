from src.etl.pipeline.stage_creator import StageCreator
from src.etl.pipeline.file_extractor import FileExtractor

from datetime import datetime
from src.etl.pipeline import config_btk
from src import snowflake_creds, aws_credentials
import re
from src.etl.pipeline import ApplyTransformations,ApplyTransformations_add_tli


    


if __name__ == "__main__":
        ds = config_btk.location.object_name
        StageCreator(config_btk).create_stage(ds)
        FileExtractor(config_btk).perform_extraction() 
        # ApplyTransformations(ds).generate_pii_master()
        # ApplyTransformations_add_tli(ds).generate_add_tli()
        