
from typing import Optional, List
import pandas as pd
import io
import time
from snowflake.snowpark.types import StructType
from ...src import logger_ltk, config_ltk
from ..utils.utilities import StorageConnector
from ..modules import session_ltk
import openpyxl

read_format = {
    "PARSE_HEADER": True,
    "FIELD_DELIMITER": "|",
    "NULL_IF": ("NULL", "NUL", ""),
    "EMPTY_FIELD_AS_NULL": True,
    "ERROR_ON_COLUMN_COUNT_MISMATCH": False,
    "ON_ERROR": "CONTINUE",
    "FIELD_OPTIONALLY_ENCLOSED_BY": '"'
}

logger = logger_ltk

class ReadFile:
    def __init__(self, config_ltk):
        self.config = config_ltk
        self.s3_client = StorageConnector(config_ltk).s3
    
    
  
    
    def read_file(self, schema: StructType):
        """Read files and return total count"""
        
        lender_name = self.config.location.lender_name
        bucket_name = self.config.location.bucket_name
        batch = self.config.location.batch
        
        if batch:
            prefix = f"lender-to-kft/{lender_name}/{batch}/"
        else:
            prefix = f"lender-to-kft/{lender_name}/"
        
        logger.info(f"Fetching from S3: s3://{bucket_name}/{prefix}")
        
        response = self.s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix
        )
        
        files = []
        if "Contents" in response:
            for obj in response["Contents"]:
                full_key = obj["Key"]
                filename = full_key.replace(prefix, "")
                if filename and not filename.endswith('/'):
                    files.append({
                        'filename': filename,
                        'full_key': full_key,
                        'size': obj['Size']
                    })
                    logger.info(f"Found: {filename} ({obj['Size']} bytes)")
        
        if not files:
            logger.error("No files found!")
         
        
        for file_info in files:
            filename = file_info['filename']
            file_path = f"@{self.config.location.stage_name}/{filename}"
            logger.info(f"Processing: {filename}")
            
            try:
                if filename.lower().endswith(('.xlsx', '.xls')):
                    logger.info(f"Reading Excel file: {filename}")
                  
                    s3_response = self.s3_client.get_object(Bucket=bucket_name, Key=file_info['full_key'])
                    file_content = s3_response['Body'].read()
                    
                  
                    df_pd = pd.read_excel(io.BytesIO(file_content), engine='openpyxl')
                    
                    df_pd.columns = [col.upper() for col in df_pd.columns]
                    
                    df = session_ltk.create_dataframe(df_pd)
                    
                    df.show()
                    
                    return df
                    
                    
                elif filename.lower().endswith('.csv'):
                    logger.info(f"Reading CSV file via Snowpark: {file_path}")
                    df = (
                        session_ltk.read
                        .options(read_format)
                        .schema(schema)
                        .csv(file_path)
                    )
                    
                    return df

                else:
                    logger.warning(f"Unsupported file format: {filename}")
                
                    
            except Exception as e:
                logger.error(f"✗ Error processing {filename}: {e}")
                import traceback
                logger.error(traceback.format_exc())
        
      
        return None