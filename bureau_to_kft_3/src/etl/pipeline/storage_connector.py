from box import ConfigBox
from ....src import aws_credentials, logger_btk
from ....src.utils.exceptions import (
    ConfigurationError,
    S3ConnectionError,
)
import boto3
from typing import Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logger_btk
class StorageConnector:
    """
     S3 storage connector
    """

    def __init__(self, config_btk: ConfigBox, max_workers: int = 10) -> None:
        self.config = config_btk
        self.credentials = aws_credentials
        self.max_workers = max_workers

        # Validate credentials
        if not self.credentials.get("aws_key") or not self.credentials.get(
            "aws_secret"
        ):
            raise ConfigurationError(
                message="AWS credentials not configured",
                config_key="aws_credentials",
                expected_type="dict with aws_key and aws_secret",
            )

        if self.config.location.batch:
            self.folder_path = f"{self.configconfig.location.object_name}/{self.config.location.batch}"
        else:
            self.folder_path = f"{self.config.location.object_name}"

        logger.info(    
            f"[S3 CONNECTION] Initializing S3 connection for path: {self.folder_path}"
        )

        try:
            self.s3 = boto3.client(
                "s3",
                aws_access_key_id=self.credentials["aws_key"],
                aws_secret_access_key=self.credentials["aws_secret"],
                region_name=self.credentials["aws_region"],
            )
            logger.info("[S3 CONNECTION] S3 client initialized successfully")
        except Exception as e:
            logger.error(f"[S3 CONNECTION] Failed to initialize S3 client: {str(e)}")
            raise S3ConnectionError(
                message="[S3 CONNECTION] Failed to initialize S3 client",
                bucket=self.config.location.bucket_name,
                original_error=e,
            )

    def get_list_of_sets_available_in_s3(self) -> Tuple[List[str], str]:
        """
        Get list of data sets available in S3 bucket.

        Returns:
            Tuple containing:
            - List of set folder names (e.g., ['set_1', 'set_2'])
            - S3 path string

        Raises:
            S3ConnectionError: If S3 access fails
        """

        logger.info(
            f"[S3 CONNECTION] Scanning S3 path: s3://{self.config.location.bucket_name}/{self.folder_path}"
        )

        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            page_iterator = paginator.paginate(
                Bucket=self.config.location.bucket_name, Prefix=self.folder_path
            )

            # Check if bucket has any contents
            # if 'Contents' not in obj:
            #     logger.warning(
            #         f"No files found in s3://{self.config.location.bucket_name}/{self.folder_path}"
            #     )
            #     return []

            all_sets = set()

            # Process pages in parallel
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []

                for page in page_iterator:
                    if "Contents" in page:
                        future = executor.submit(self._extract_set_names, page)
                        futures.append(future)

                # Collect results
                for future in as_completed(futures):
                    try:
                        batches = future.result()
                        all_sets.update(batches)
                    except Exception as e:
                        logger.error(f"[S3 CONNECTION] Error processing page: {e}")

            ls = list(all_sets)

            if not ls:
                logger.warning(
                    f"[S3 CONNECTION] No 'set_*' folders found in "
                    f"s3://{self.config.location.bucket_name}/{self.folder_path}"
                )
            else:
                logger.info(
                    f"[S3 CONNECTION] Found {len(ls)} set folders: {', '.join(sorted(ls))}"
                )

            logger.info(f"[S3 CONNECTION] Completed scanning S3 path.")
            logger.info(f"\n{'-'*100}")

            return ls, f"s3://{self.config.location.bucket_name}/{self.folder_path}"

        except self.s3.exceptions.NoSuchBucket as e:
            logger.error(
                f"[S3 CONNECTION] S3 bucket not found: {self.config.location.bucket_name}"
            )
            raise S3ConnectionError(
                message=f"[S3 CONNECTION] S3 bucket does not exist",
                bucket=self.config.location.bucket_name,
                key=self.folder_path,
                original_error=e,
            )
        except self.s3.exceptions.NoSuchKey as e:
            logger.error(f"[S3 CONNECTION] S3 path not found: {self.folder_path}")
            raise S3ConnectionError(
                message=f"[S3 CONNECTION] S3 path does not exist",
                bucket=self.config.location.bucket_name,
                key=self.folder_path,
                original_error=e,
            )
        except Exception as e:
            logger.error(f"[S3 CONNECTION] Failed to list S3 objects: {str(e)}")
            raise S3ConnectionError(
                message="[S3 CONNECTION] Failed to list objects in S3 bucket",
                bucket=self.config.location.bucket_name,
                key=self.folder_path,
                original_error=e,
            )

    @staticmethod
    def _extract_set_names(page: Dict) -> set:
        """Extract set names from S3 page contents"""
        sets = set()

        for item in page["Contents"]:
            file_name = item["Key"].split("/")[-1]
            if file_name:
                sets.add(file_name)
    
        return sets

    def get_available_batches(self, prefix_override: str = None) -> List[str]:
        """
        Get list of available batches (subdirectories) in the configured S3 bucket.
        
        Args:
            prefix_override: Optional prefix to use instead of config.location.object_name
            
        Returns:
            List of batch names
        """
        # Use provided prefix or fall back to config, ensuring it ends with /
        prefix = prefix_override if prefix_override else self.config.location.object_name
        if not prefix.endswith('/'):
            prefix += '/'
            
        logger.info(
            f"[S3 CONNECTION] Listing batches in: s3://{self.config.location.bucket_name}/{prefix}"
        )

        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            # Use Delimiter='/' to simulate directory listing
            page_iterator = paginator.paginate(
                Bucket=self.config.location.bucket_name, 
                Prefix=prefix, 
                Delimiter='/'
            )

            batches = set()
            for page in page_iterator:
                # CommonPrefixes contains the "subdirectories"
                if "CommonPrefixes" in page:
                    for p in page["CommonPrefixes"]:
                        # Extract the folder name from the prefix
                        # e.g., "bureau-to-kft/equifax/batch_1/" -> "batch_1"
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
            # Return empty list instead of raising to avoid crashing UI
            return []
