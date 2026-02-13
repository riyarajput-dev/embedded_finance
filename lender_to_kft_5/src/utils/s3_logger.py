
import logging
import boto3
from logging.handlers import RotatingFileHandler
from datetime import datetime


class S3LoggingHandler(logging.Handler):
    def __init__(self, bucket_name, log_file_name):
        super().__init__()
        self.bucket_name = bucket_name
        self.log_file_name = log_file_name
        self.s3_client = boto3.client('s3')
        

    def emit(self, record):
        # Format the log record
        log_message = self.format(record)
        
        # Write the log message to S3 file
        try:
            # Check if the file already exists on S3 (if not, create it)
            # Open the file for appending
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.log_file_name)
            current_logs = response['Body'].read().decode('utf-8')
        except self.s3_client.exceptions.NoSuchKey:
            current_logs = ""

        # Add the new log entry to the existing logs
        updated_logs = current_logs + log_message + "\n"
        
        # Upload the updated logs back to S3
        self.s3_client.put_object(Bucket=self.bucket_name, Key=self.log_file_name, Body=updated_logs)

# Set up the logger
def setup_logger(config_ltk):
    logger = logging.getLogger("S3Logger")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    # Forcefully remove existing handlers to prevent duplication on reloads
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Create the custom S3 logging handler
    bucket_name = "lenders-data-exchange"
    log_file_name = f'lender-to-kft/{config_ltk.location.lender_name}/{config_ltk.location.log_file}'  

    s3_handler = S3LoggingHandler(bucket_name, log_file_name)

    # Create a logging format
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    s3_handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(s3_handler)

    return logger

