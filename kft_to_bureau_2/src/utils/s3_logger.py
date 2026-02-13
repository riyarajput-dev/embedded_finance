import logging
import boto3
import os
from datetime import datetime

class S3LoggingHandler(logging.Handler):
    def __init__(self, bucket_name, log_file_name):
        super().__init__()
        self.bucket_name = bucket_name
        self.log_file_name = log_file_name
        
        # Explicitly pass credentials from environment
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_KEY"),
            aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
            region_name=os.getenv("AWS_REGION", "ap-south-1")
        )

    def emit(self, record):
        try:
            log_message = self.format(record)
            
            # 1. Fetch current logs
            try:
                response = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.log_file_name)
                current_logs = response['Body'].read().decode('utf-8')
            except self.s3_client.exceptions.NoSuchKey:
                current_logs = ""
            except Exception as e:
                # Fallback if S3 fails (to prevent app from crashing)
                return

            # 2. Append and Put back
            updated_logs = current_logs + log_message + "\n"
            
            self.s3_client.put_object(
                Bucket=self.bucket_name, 
                Key=self.log_file_name, 
                Body=updated_logs,
                ContentType='text/plain'
            )
        except Exception as e:
            # Prevent logging errors from crashing the main application
            pass

# Set up the logger
def setup_s3_logger(): 
    # Use a specific name to avoid collision with root logger
    logger = logging.getLogger("sf_to_bureau_logger")
    logger.setLevel(logging.INFO)

    # Prevent duplicate handlers if setup is called multiple times
    if not logger.handlers:
        bucket_name = "bureau-data-exchange"
        log_file_name = "kft-to-bureau/equifax/logs/server_log.log"

        s3_handler = S3LoggingHandler(bucket_name, log_file_name)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        s3_handler.setFormatter(formatter)
        logger.addHandler(s3_handler)

    return logger

