
from box import ConfigBox
import boto3
from typing import Dict, List, Iterator, Tuple
import yaml
from ...src import logger_ltk
from ..utils import aws_credentials
from ...src import config_ltk
import re
from datetime import datetime
import pandas as pd
from io import StringIO

_S3_CLIENT = None
logger = logger_ltk
class StorageConnector:
    def __init__(self, config: ConfigBox) -> None:
        global _S3_CLIENT
        self.config = config
        self.credentials = aws_credentials
        
        if not self.credentials.get("aws_key") or not self.credentials.get(
            "aws_secret"
        ):
            logger.info("credentials not available")


        self.fetch_folder_path = f"lender-to-kft/{config.location.lender_name}"

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
        
    def get_available_batches(self, prefix_override: str = None) -> List[str]:
        """
        Get list of available batches (subdirectories) in the configured S3 bucket.
        
        Args:
            prefix_override: Optional prefix to use instead of the default prefix
            
        Returns:
            List of batch names
        """
        lender_name = self.config.location.lender_name
        bucket_name = self.config.location.bucket_name
        
        # Use provided prefix or fall back to default
        prefix = prefix_override if prefix_override else f"lender-to-kft/{lender_name}/"
        if not prefix.endswith('/'):
            prefix += '/'
            
        logger.info(
            f"[S3 CONNECTION] Listing batches in: s3://{bucket_name}/{prefix}"
        )

        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            # Use Delimiter='/' to simulate directory listing
            page_iterator = paginator.paginate(
                Bucket=bucket_name, 
                Prefix=prefix, 
                Delimiter='/'
            )

            batches = set()
            for page in page_iterator:
                # CommonPrefixes contains the "subdirectories"
                if "CommonPrefixes" in page:
                    for p in page["CommonPrefixes"]:
                        # Extract the folder name from the prefix
                        # e.g., "lender-to-kft/abfcl/batch_1/" -> "batch_1"
                        folder_path = p["Prefix"]
                        # Remove the main prefix to get the relative path
                        relative_path = folder_path[len(prefix):]
                        # Remove trailing slash
                        batch_name = relative_path.rstrip('/')
                        if batch_name:
                            batches.add(batch_name)
            
            batch_list = sorted(list(batches))
            logger.info(f"[S3 CONNECTION] Found {len(batch_list)} batches: {', '.join(batch_list)}")
            return batch_list

        except Exception as e:
            logger.error(f"[S3 CONNECTION] Failed to list batches: {str(e)}")
            return []
        


