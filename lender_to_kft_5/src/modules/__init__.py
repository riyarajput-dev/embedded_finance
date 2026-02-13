
from snowflake.snowpark import DataFrame
import snowflake.snowpark.functions as F
from ...src import snowflake_connection_obj, config_ltk
from ...src import logger_ltk
from datetime import date


try:
    session_ltk = snowflake_connection_obj(config_ltk.database, config_ltk.schema)
except Exception as e:
    logger_ltk.info(f"Snowflake connection failed: {e}")


def save_table(
    df: DataFrame,
    table_name: str,
    mode: str,
    table_type: str = ""
 
):
    

    column_order = "name"
    df.write.save_as_table(
        table_name,mode=mode,table_type=table_type, column_order=column_order
    )
  