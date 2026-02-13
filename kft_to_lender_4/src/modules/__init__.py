
import os
import sys
from pathlib import Path
project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
from snowflake.snowpark import DataFrame
import snowflake.snowpark.functions as F
from ...src import  snowflake_connection_obj, config_ktl
from ...src import logger_ktl
from datetime import date


try:
    session_ktl = snowflake_connection_obj(config_ktl.database, config_ktl.schema)
except Exception as e:
    logger_ktl.info(f"Snowflake connection failed: {e}")


def save_table(
    df: DataFrame,
    table_name: str,
   
    mode: str,
    table_type: str = ""
 
):
    
    df = df.with_column("created_at", F.to_date(F.lit(f"{date.today()}")))

    column_order = "name"
    df.write.save_as_table(
        table_name,mode=mode,table_type=table_type, column_order=column_order
    )
    
def save_lenders_table(
    df: DataFrame,
    table_name: str,
    mode: str,
    lenders_name : str,
    bureau_name:str,
    table_type: str = "",
):
   
    df = df.with_column("created_at", F.to_date(F.lit(f"{date.today()}")))
    df = df.with_column("lenders_name", F.lit(f"{lenders_name}"))
    df = df.with_column("bureau_type", F.lit(f"{bureau_name}"))
    df = df.with_column("lender_batch", F.lit(None))

       
    column_order = "name" if mode == 'append' else "index"
    df.write.save_as_table(
        table_name,mode=mode,table_type=table_type, column_order=column_order
    )
    