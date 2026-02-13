import re
import boto3
import os
from .. import config_ktb, logger_ktb
from typing import Dict, List, Iterator, Tuple
from datetime import datetime 

aws_credentials = {
    "aws_key": os.getenv("AWS_KEY"),
    "aws_secret": os.getenv("AWS_SECRET_KEY"),
    "aws_region":os.getenv("AWS_REGION")
}

class S3Connector:
    def __init__(self,config_ktb):
        self.config = config_ktb
        self.credentials = aws_credentials
        
        if not self.credentials.get("aws_key") or not self.credentials.get(
            "aws_secret"
        ):
            logger_ktb.info("credentials not available")


        self.fetch_folder_path = f"{self.config.location.folder_name}/{self.config.location.object_name}"
        self.push_folder_path =f"{self.config.location.folder_name}/{self.config.location.push_object_name}"

        logger_ktb.info(
            f"[S3 CONNECTION] Initializing S3 connection for fetch path: {self.fetch_folder_path}"
        )

        try:
            self.s3 = boto3.client(
                "s3",
                aws_access_key_id=self.credentials["aws_key"],
                aws_secret_access_key=self.credentials["aws_secret"],
                region_name=self.credentials["aws_region"],
            )
            logger_ktb.info("[S3 CONNECTION] S3 client initialized successfully")
        except Exception as e:
            logger_ktb.error(f"[S3 CONNECTION] Failed to initialize S3 client: {str(e)}")
            
        
    import boto3

    def list_s3_folders(bucket_name):
        s3 = boto3.client('s3')

        response = s3.list_objects_v2(
            Bucket=bucket_name,
            Delimiter='/'
        )

        folders = []

        if 'CommonPrefixes' in response:
            for prefix in response['CommonPrefixes']:
                folder_name = prefix['Prefix']
                folders.append(folder_name)

        return folders

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
            logger_ktb.info(f"[ANALYTICS] Saved JSON to s3://{bucket_name}/analytics/{datetime.now().strftime('%Y-%m-%d')}/{filename}")
            return True
            
        except Exception as e:
            logger_ktb.error(f"[ANALYTICS] Failed to save JSON: {str(e)}")
            return False

   

    def set_batch_number(self):
            
        bucket_name = self.config.location.bucket_name
        object_name = self.config.location.log_file
        
        try:
            response = self.s3.get_object(Bucket=bucket_name, Key=object_name)
            current_logs = response['Body'].read().decode('utf-8')
        except self.s3.exceptions.NoSuchKey:
            current_logs = ""
            
        # Updated pattern to be more robust: 
        # Handles "Scrub_Batch_no.: 3", "scrub_batch_no: 3", "Scrub_Batch_no = 3", etc.
        batch_no_pattern = r"scrub_batch_no\.?[:=]\s*(\d+)"

        batch_numbers = re.findall(batch_no_pattern, current_logs, re.IGNORECASE)

        if not batch_numbers:
            return 1
        
        batch_numbers = [int(batch) for batch in batch_numbers if batch.isdigit()]
        
        if not batch_numbers:
            return 1
        
        max_batch_no = max(batch_numbers)
        
        return max_batch_no + 1
    
    