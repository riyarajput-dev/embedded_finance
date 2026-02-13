

import os
import sys
from pathlib import Path
project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
from box import ConfigBox
import boto3
from typing import Dict, List, Iterator, Tuple
import yaml
from .. import logger_ktl
from ..utils import aws_credentials
from .. import config_ktl
import re
from datetime import datetime
import pandas as pd
from io import StringIO

_S3_CLIENT = None
logger = logger_ktl
class StorageConnector:
 
    def __init__(self, config: ConfigBox) -> None:
        global _S3_CLIENT
        self.config = config
        self.credentials = aws_credentials
        
        if not self.credentials.get("aws_key") or not self.credentials.get(
            "aws_secret"
        ):
            logger.info("credentials not available")


        self.fetch_folder_path = f"{config.location.folder_name}/{config.location.fetch_object_name}"
        self.push_folder_path =f"{config.location.folder_name}/{config.location.push_object_name}"

        if _S3_CLIENT is None:
            logger.info(
                f"[S3 CONNECTION] Initializing S3 connection for fetch path: {self.fetch_folder_path}"
            )

            try:
                _S3_CLIENT = boto3.client(
                    "s3",
                    aws_access_key_id=self.credentials["aws_key"],
                    aws_secret_access_key=self.credentials["aws_secret"],
                    region_name=self.credentials["aws_region"],
                )
                logger.info("[S3 CONNECTION] S3 client initialized successfully")
            except Exception as e:
                logger.error(f"[S3 CONNECTION] Failed to initialize S3 client: {str(e)}")
        
        self.s3 = _S3_CLIENT
            

  
  
    def set_batch_number(self, folder_name):

        bucket_name = self.config.location.bucket_name
        object_name = f"kft-to-lender/{folder_name}/{self.config.location.log_file}"
        
        try:
            response = self.s3.get_object(Bucket=bucket_name, Key=object_name)
            current_logs = response['Body'].read().decode('utf-8')
        except self.s3.exceptions.NoSuchKey:
            current_logs = ""
        
        if not current_logs:
            return 1
                
        batch_no_pattern = r"lender_batch_no\.?[:=]\s*(\d+)"

        batch_numbers = re.findall(batch_no_pattern, current_logs, re.IGNORECASE)

        if not batch_numbers:
            return 1
        
        batch_numbers = [int(batch) for batch in batch_numbers if batch.isdigit()]
        
        if not batch_numbers:
            return 1
        
        max_batch_no = max(batch_numbers)
        
        return max_batch_no + 1
        
        
    def upload_s3(self, bucket_name, folder_name, filename, content):
        try:
            self.s3.put_object(Bucket=bucket_name, Key=f"{folder_name}/{filename}", Body=content)
        except Exception as e:
            logger.error(f"Failed to upload file: {folder_name}/{filename} to bucket: {bucket_name}. Error: {str(e)}")
        
    def save_analytics(self, json_data: Dict, filename: str):
        """
        Save JSON data to S3 with the specified filename.
        
        Args:
            json_data: Dictionary containing the data to save as JSON
            filename: Name of the JSON file to save in the bucket
            
        Returns:
            bool: True if successful, False otherwise
        """
        bucket_name = self.config.location.bucket_name
        
        try:
            import json
            # Convert dictionary to JSON string
            json_string = json.dumps(json_data, indent=2)
            json_bytes = json_string.encode('utf-8')
            
            # Upload to S3
            self.s3.put_object(Bucket=bucket_name, Key=f"analytics/{filename}", Body=json_bytes)
            logger.info(f"[ANALYTICS] Saved JSON to s3://{bucket_name}/analytics/{datetime.now().strftime('%Y-%m-%d')}/{filename}")
            return True
            
        except Exception as e:
            logger.error(f"[ANALYTICS] Failed to save JSON: {str(e)}")
            return False
        
   