from snowflake.snowpark import DataFrame
import snowflake.snowpark.functions as F
from .. import  snowflake_connection_obj
from .. import logger_ptk, config_ptk
from datetime import date


try:
    session_ptk = snowflake_connection_obj(config_ptk.database, config_ptk.schema)
except Exception as e:
    logger_ptk.info(f"Snowflake connection failed: {e}")


def save_table(
    df: DataFrame,
    table_name: str,
    batch:int,
    partner_name: str,
    mode: str,
    table_type: str = ""
 
):
    
    df = df.with_column("created_at", F.to_date(F.lit(f"{date.today()}")))
    df = df.with_column("sent_to_scrub", F.lit(False))
    df = df.with_column("sent_to_equifax", F.lit(False))
    df = df.with_column("sent_to_experian", F.lit(False))
    df = df.with_column("scrub_date", F.lit(None))
    df = df.with_column("batch_no", F.lit(f"batch_{batch}")) 
    df = df.with_column("partner_name", F.lit(f"{partner_name}")) 
    
    
    column_order = "name"
    df.write.save_as_table(
        table_name,mode=mode,table_type=table_type, column_order=column_order
    )
    
def save_raw_table(
    df: DataFrame,
    table_name: str,
    partner_name:str,
    batch: int,
    file_name: str,
    mode: str,
    table_type: str = ""
 
): 
    # Standardize metadata columns to UPPERCASE to avoid Snowflake identifier errors
    if "BATCH_NO" not in df.columns:
        df = df.with_column("BATCH_NO", F.lit(f"batch_{batch}")) 
    if "PARTNER_NAME" not in df.columns:
        df = df.with_column("PARTNER_NAME", F.lit(f"{partner_name}")) 
    if "FILENAME" not in df.columns:
        df = df.with_column("FILENAME", F.lit(f"{file_name}")) 
    if "CREATED_AT" not in df.columns:
        df = df.with_column("CREATED_AT", F.to_date(F.lit(f"{date.today()}")))
    if "STATUS" not in df.columns:
        df = df.with_column("STATUS", F.lit("not processed"))
        
    column_order = "name"
    df.write.save_as_table(
        table_name,mode=mode,table_type=table_type, column_order=column_order
    )
    
            
def save_locally(df, filename, partner_name, batch_no, file_name, mode="append"):
    """
    Save dataframe locally for testing without Snowflake ingestion.
    
    Args:
        df: Snowflake dataframe to save
        filename: Original filename being processed
        partner_name: Partner/folder name
        batch_no: Batch number
        mode: 'append' or 'overwrite' (default: 'append')
    """
    import os
    from datetime import datetime
    
    # Create output directory if it doesn't exist
    output_dir = "output/local_test"
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate filename with timestamp and batch info
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_filename = f"{partner_name}_batch{batch_no}_{timestamp}.csv"
    local_filepath = os.path.join(output_dir, local_filename)
    
    
    
    # Convert Snowflake dataframe to Pandas and save
    pandas_df = df.to_pandas()
    pandas_df['filename'] = file_name   
    pandas_df['batch_no'] = batch_no 
    if mode == "append" and os.path.exists(local_filepath):
        # Append to existing file
        pandas_df.to_csv(local_filepath, mode='a', header=False, index=False)
    else:
        # Create new file or overwrite
        pandas_df.to_csv(local_filepath, index=False)
    
    logger_ptk.info(f"Saved locally to: {local_filepath}")
    logger_ptk.info(f"Records saved: {len(pandas_df)}")
    
    return local_filepath
    
    
read_format = {
        "PARSE_HEADER": True,
        "FIELD_DELIMITER": "|",
        "NULL_IF": ("NULL", "NUL", ""),
        "EMPTY_FIELD_AS_NULL": True,
        "ERROR_ON_COLUMN_COUNT_MISMATCH": True,
        "ON_ERROR": "CONTINUE","FIELD_OPTIONALLY_ENCLOSED_BY": '"'         
}

