

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
from .. import logger_ptk
from ..utils import aws_credentials
from .. import config_ptk
import re
from datetime import datetime
import pandas as pd
from io import StringIO

_S3_CLIENT = None

class StorageConnector:
 
    def __init__(self, config_ptk: ConfigBox) -> None:
        global _S3_CLIENT
        self.config_ptk = config_ptk
        self.credentials = aws_credentials
        
        if not self.credentials.get("aws_key") or not self.credentials.get(
            "aws_secret"
        ):
            logger_ptk.info("credentials not available")


        self.fetch_folder_path = f"{config_ptk.location.folder_name}/{config_ptk.location.fetch_object_name}"
        self.push_folder_path =f"{config_ptk.location.folder_name}/{config_ptk.location.push_object_name}"

        if _S3_CLIENT is None:
            logger_ptk.info(
                f"[S3 CONNECTION] Initializing S3 connection for fetch path: {self.fetch_folder_path}"
            )

            try:
                _S3_CLIENT = boto3.client(
                    "s3",
                    aws_access_key_id=self.credentials["aws_key"],
                    aws_secret_access_key=self.credentials["aws_secret"],
                    region_name=self.credentials["aws_region"],
                )
                logger_ptk.info("[S3 CONNECTION] S3 client initialized successfully")
            except Exception as e:
                logger_ptk.error(f"[S3 CONNECTION] Failed to initialize S3 client: {str(e)}")
        
        self.s3 = _S3_CLIENT
            

    def fetch_files_queue(self, file_keys) -> Iterator[Tuple[str, bytes]]:
        """Yield tuples of (filename, bytes) for the provided S3 object keys."""
        bucket_name = self.config_ptk.location.bucket_name
        for file_key in file_keys:
            try:
                response = self.s3.get_object(Bucket=bucket_name, Key=file_key)
                body = response['Body'].read()
                yield file_key, body
            except Exception as e:
                logger_ptk.error(f"[S3 CONNECTION] Failed to fetch {file_key}: {e}")
                continue

    def upload_files(self, file_obj, dest_filename: str = None):
        bucket_name = self.config_ptk.location.bucket_name
        filename = dest_filename
        object_name = f"{self.fetch_folder_path.rstrip('/')}/{os.path.basename(filename)}"

        if isinstance(file_obj, (bytes, bytearray)):
            self.s3.put_object(Bucket=bucket_name, Key=object_name, Body=file_obj)
        else:
            logger_ptk.error("failed to upload files")
            return None, None, None

        logger_ptk.info(f"Uploaded object to s3://{bucket_name}/{object_name}")
        # Return the S3 key, the original filename, and the uploaded bytes (content)
        logger_ptk.info(f"object_name:{object_name},filename: {filename}")
        return object_name, filename, file_obj
    
    

    def set_batch_number(self, s3_folder, today):
        """
        Determine batch number based on date and folder from logs.
        
        Args:
            s3_folder: Current S3 folder being processed
            today: Current date string (should be in 'YYYY-MM-DD' format) or datetime object
            
        Returns:
            int: Batch number to use
        """
        bucket_name = self.config_ptk.location.bucket_name
        object_name = self.config_ptk.location.log_file
        
        try:
            response = self.s3.get_object(Bucket=bucket_name, Key=object_name)
            current_logs = response['Body'].read().decode('utf-8')
        except self.s3.exceptions.NoSuchKey:
            current_logs = ""
        
        if not current_logs:
            return 1
        
        log_lines = current_logs.strip().split('\n')
        
        # Updated Regex to handle "Partner" vs "partner" and "Batch no:" vs "batch:"
        log_pattern = r"(?i)partner:\s*([^,]+),\s*batch(?: no)?:\s*(\d+),\s*date:\s*(.+)"
        
        # Track batch numbers per folder
        folder_batches = {}
        
        for line in reversed(log_lines):  # Process from newest to oldest
            match = re.search(log_pattern, line)
            if match:
                logged_folder = match.group(1).strip()
                logged_batch = int(match.group(2))
                logged_date_str = match.group(3).strip()
                
                try:
                    # Using split to take just the YYYY-MM-DD part for date comparison
                    logged_date_part = logged_date_str.split(' ')[0]
                    logged_date = datetime.strptime(logged_date_part, "%Y-%m-%d").date()
                except ValueError:
                    continue
                
                if logged_folder not in folder_batches:
                    folder_batches[logged_folder] = {
                        'batch': logged_batch,
                        'date': logged_date
                    }
        
        # If 'today' is already a datetime object, convert it to date
        if isinstance(today, datetime):
            today_date = today.date()
        else:
            # Convert 'today' string to datetime.date object
            try:
                today_date = datetime.strptime(today.split(' ')[0], "%Y-%m-%d").date()
            except ValueError:
                today_date = datetime.strptime(today, "%Y-%m-%d").date()
        
        # Check if current folder exists in logs
        if s3_folder in folder_batches:
            last_entry = folder_batches[s3_folder]
            
            # Same date and same folder -> keep same batch number
            if last_entry['date'] == today_date:
                return last_entry['batch']
            else:
                # Different date -> increment batch number
                return last_entry['batch'] + 1
        else:
            # New folder -> start with batch 1
            return 1
        
    def save_analytics(self, json_data: Dict, filename: str):
        """
        Save JSON data to S3 with the specified filename.
        
        Args:
            json_data: Dictionary containing the data to save as JSON
            filename: Name of the JSON file to save in the bucket
            
        Returns:
            bool: True if successful, False otherwise
        """
        bucket_name = self.config_ptk.location.bucket_name
        
        try:
            import json
            # Convert dictionary to JSON string
            json_string = json.dumps(json_data, indent=2)
            json_bytes = json_string.encode('utf-8')
            
            # Upload to S3
            self.s3.put_object(Bucket=bucket_name, Key=f"analytics/{filename}", Body=json_bytes)
            logger_ptk.info(f"[ANALYTICS] Saved JSON to s3://{bucket_name}/analytics/{datetime.now().strftime('%Y-%m-%d')}/{filename}")
            return True
            
        except Exception as e:
            logger_ptk.error(f"[ANALYTICS] Failed to save JSON: {str(e)}")
            return False
        
        
    # def set_batch_number(self):
        
    #     bucket_name = self.config_ptk.location.bucket_name
    #     object_name = self.config_ptk.location.log_file
        
    #     try:
    #         response = self.s3.get_object(Bucket=bucket_name, Key=object_name)
    #         current_logs = response['Body'].read().decode('utf-8')
    #     except self.s3.exceptions.NoSuchKey:
    #         current_logs = ""
            
    #     batch_no_pattern = r"Batch no\. = (\S+)"

    #     batch_numbers = re.findall(batch_no_pattern, current_logs)

    #     if not batch_numbers:
    #         return 1
        
    #     batch_numbers = [int(batch) for batch in batch_numbers if batch.isdigit()]
        
    #     if not batch_numbers:
    #         return 1
        
    #     max_batch_no = max(batch_numbers)
        
    #     return max_batch_no + 1
    

    