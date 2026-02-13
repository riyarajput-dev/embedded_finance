from snowflake.snowpark import DataFrame
import snowflake.snowpark.functions as F
from ....src import scrub_dt, snowflake_connection_obj, logger_btk, config_path, config_btk
from ...utils.utilities import generate_ascii_art, lender_data_s3_push
from ...utils.exceptions import SnowflakeConnectionError
from ....lender_scripts.zype_experian import mappings

from ....lender_scripts.zype_equifax import zype_eq_mappings
from ....lender_scripts.prefr import prefr_mappings
from datetime import date

logger = logger_btk
logger.info(f"reading config from {config_path}")

try:
    session_btk = snowflake_connection_obj(config_btk.database, config_btk.schema)
    logger.info("Snowflake connection established successfully")
except:
    raise SnowflakeConnectionError(
        message= "Error while connecting to Snowflake",
        database= config_btk.database,
        schema= config_btk.schema
    )


generate_ascii_art("KFT ETL")


from ...etl.pipeline.constants import *
from ...etl.pipeline.queries import *



def save_table(
    df: DataFrame,
    table_name: str,
    mode: str,
    batch_no: str = None,
    table_type: str = "",
):
    if batch_no is not None:
        df = df.with_column("batch_no", F.lit(f"{batch_no}"))
        df = df.with_column("created_at", F.to_date(F.lit(f"{scrub_dt}")))

       
    column_order = "name" if mode == 'append' else "index"
    df.write.save_as_table(
        table_name,mode=mode,table_type=table_type, column_order=column_order
    )
    



class ApplyTransformations:
    def __init__(self, dataset: str, config_obj=None) -> None:
        self.config = config_obj if config_obj else config_btk
        self.batch_no = f"{self.config.location.batch}".lower()
        logger.info(f"fetching data with identity tag {self.batch_no}.")
        self.demog = session_btk.sql(
            get_pii_master_query(self.batch_no)
        )
        
    def generate_pii_master(self):
        config_mod = self.config.table_name
        logger.info("generating  pii master file  by adding all pii files and score file ")
        # self.demog = self.demog.withColumnRenamed('PAN_NUMBER','PAN')
        # self.demog = self.demog.dropDuplicates(["CUSTOMER_ID"])
        self.demog = self.demog.withColumn(
            "status",
            F.when(
                F.col("SCORE") >= 700,
                F.lit("Approved"),
            ).otherwise(F.lit("Declined")),
        )
        save_table(
            self.demog,
            config_mod.pii_master,
            config_mod.mode,
            self.batch_no,
        )
        logger.info(f"records saved : {self.demog.count()}")
        logger.info("pii master file  uploaded successfully.")

class ApplyTransformations_add_tli:
    def __init__(self, dataset: str, config_obj=None) -> None:
        self.config = config_obj if config_obj else config_btk
        self.batch_no = f"{self.config.location.batch}".lower()
        logger.info(f"fetching data with identity tag {self.batch_no}.")
        self.demog = session_btk.sql(
            get_additional_attr_tli(self.batch_no)
        )
       
    def generate_add_tli(self):
        config_mod = self.config.table_name
        logger.info("generating  add_tli  by flatten AR file ")
        
       
        save_table(
            self.demog,
            config_mod.add_tli,
            config_mod.mode,
            self.batch_no,
        )
        logger.info(f"records saved : {self.demog.count()}")
        logger.info("add_tli file  uploaded successfully.")