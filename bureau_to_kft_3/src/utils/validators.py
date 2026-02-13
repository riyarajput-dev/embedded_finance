"""
Data validation utilities for the S3 to Snowflake ETL pipeline.

This module provides comprehensive validation functions for schema,
data types, formats, and data quality checks.
"""

import re
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from snowflake.snowpark import DataFrame
from snowflake.snowpark.types import StructType, StructField
import snowflake.snowpark.functions as F

from ..utils.exceptions import (
    PipelineException,
    S3ConnectionError,
    SnowflakeConnectionError,
    SchemaValidationError,
    DataValidationError,
    FileFormatError,
    ConfigurationError,
    DataQualityError
)
from ...src import logger_btk
from pathlib import Path

logger = logger_btk


class SchemaValidator:
    """Validates file schema against expected models."""
    
    @staticmethod
    def validate_schema(
        actual_columns: List[str],
        expected_schema: StructType,
        file_name: str,
        strict: bool = False
    ) -> Tuple[bool, List[str], List[str]]:
        """
        Validate actual columns against expected schema.
        
        Args:
            actual_columns: List of actual column names from file
            expected_schema: Expected StructType schema
            file_name: Name of file being validated
            strict: If True, extra columns cause validation failure
            
        Returns:
            Tuple of (is_valid, missing_columns, extra_columns)
            
        Raises:
            SchemaValidationError: If validation fails
        """
        expected_columns = [field.name for field in expected_schema.fields]
        
        # Find missing and extra columns
        missing_columns = [col for col in expected_columns if col not in actual_columns]
        extra_columns = [col for col in actual_columns if col not in expected_columns]
        
        # Check for required fields
        required_fields = [
            field.name for field in expected_schema.fields 
            if not field.nullable
        ]
        missing_required = [col for col in required_fields if col in missing_columns]
        
        if missing_required:
            logger.error(
                f"Schema validation failed for {file_name}: "
                f"Missing required columns: {missing_required}"
            )
            raise SchemaValidationError(
                message=f"Missing required columns in file",
                file_name=file_name,
                expected_schema=", ".join(expected_columns),
                actual_schema=", ".join(actual_columns),
                missing_columns=missing_required
            )
        
        if strict and extra_columns:
            logger.error(
                f"Schema validation failed for {file_name}: "
                f"Unexpected columns found: {extra_columns}"
            )
            raise SchemaValidationError(
                message=f"Unexpected columns found in file",
                file_name=file_name,
                expected_schema=", ".join(expected_columns),
                actual_schema=", ".join(actual_columns),
                extra_columns=extra_columns
            )
        
        if missing_columns:
            logger.warning(
                f"Schema validation warning for {file_name}: "
                f"Missing optional columns: {missing_columns}"
            )
        
        if extra_columns and not strict:
            logger.warning(
                f"Schema validation warning for {file_name}: "
                f"Extra columns will be ignored: {extra_columns}"
            )
        
        is_valid = len(missing_required) == 0 and (not strict or len(extra_columns) == 0)
        return is_valid, missing_columns, extra_columns

class FileFormatValidator:
    """Validates file format and structure."""
    
    @staticmethod
    def validate_file_format(
        file_path: str,
        expected_format: List = [".csv", ".txt", ".xlsx"]
    ) -> bool:
        """
        Validate file extension.
        
        Args:
            file_path: Path to the file
            expected_format: Expected file extension
            
        Returns:
            True if valid
            
        Raises:
            FileFormatError: If format is invalid
        """
        file_name = Path(file_path).name
        actual_format = Path(file_path).suffix.lower()
        
        if actual_format not in expected_format:
            error_msg = f"Invalid file format for {file_name}. Expected {expected_format}, got {actual_format}"
            logger.error(error_msg)
            raise FileFormatError(
                message=error_msg,
                file_name=file_name,
                expected_format=expected_format
            )
        return True

class S3Validator:
    """Validates S3 connection and paths."""
    
    @staticmethod
    def validate_s3_object(s3_client: Any, bucket: str, key: str) -> bool:
        """
        Validate that an S3 object exists and is accessible.
        
        Args:
            s3_client: Boto3 S3 client
            bucket: S3 bucket name
            key: S3 key
            
        Returns:
            True if accessible
            
        Raises:
            S3ConnectionError: If object is not accessible
        """
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
            return True
        except Exception as e:
            error_msg = f"Could not access S3 object: s3://{bucket}/{key}"
            logger.error(error_msg)
            raise S3ConnectionError(
                message=error_msg,
                bucket=bucket,
                key=key,
                original_error=e
            )

class SnowflakeValidator:
    """Validates Snowflake connection and state."""
    
    @staticmethod
    def validate_connection(session_btk: Any) -> bool:
        """
        Validate that the Snowflake session is active.
        
        Args:
            session: Snowpark Session
            
        Returns:
            True if active
            
        Raises:
            SnowflakeConnectionError: If session is invalid
        """
        try:
            session_btk.sql("SELECT 1").collect()
            return True
        except Exception as e:
            error_msg = "Snowflake connection validation failed"
            logger.error(error_msg)
            raise SnowflakeConnectionError(
                message=error_msg,
                original_error=e
            )

class DataValidator:
    """Validates data content and types."""
    
    @staticmethod
    def validate_not_null(
        df: DataFrame,
        columns: List[str],
        file_name: Optional[str] = None
    ) -> bool:
        """
        Validate that specified columns do not contain nulls.
        
        Args:
            df: Snowpark DataFrame
            columns: List of columns to check
            file_name: Name of file for context
            
        Returns:
            True if no nulls found
            
        Raises:
            DataValidationError: If nulls are found
        """
        for col in columns:
            null_count = df.filter(F.col(col).is_null()).count()
            if null_count > 0:
                error_msg = f"Null values found in required column: {col}"
                logger.error(error_msg)
                raise DataValidationError(
                    message=error_msg,
                    file_name=file_name,
                    column_name=col,
                    validation_rule="NOT NULL"
                )
        return True

class ConfigValidator:
    """Validates configuration parameters."""
    
    @staticmethod
    def validate_required_config(
        config: Dict[str, Any],
        required_keys: List[str]
    ) -> bool:
        """
        Validate that required keys exist in configuration.
        
        Args:
            config: Configuration dictionary
            required_keys: List of required keys
            
        Returns:
            True if all keys exist
            
        Raises:
            ConfigurationError: If keys are missing
        """
        for key in required_keys:
            if key not in config or config[key] is None:
                error_msg = f"Missing required configuration key: {key}"
                logger.error(error_msg)
                raise ConfigurationError(
                    message=error_msg,
                    config_key=key
                )
        return True

class QualityValidator:
    """Validates data quality metrics."""
    
    @staticmethod
    def validate_failure_threshold(
        failed_count: int,
        total_count: int,
        threshold: float,
        check_name: str,
        file_name: Optional[str] = None
    ) -> bool:
        """
        Validate that failure rate is below threshold.
        
        Args:
            failed_count: Number of failed records
            total_count: Total number of records
            threshold: Maximum allowed failure rate (0.0 to 1.0)
            check_name: Name of quality check
            file_name: Name of file for context
            
        Returns:
            True if within threshold
            
        Raises:
            DataQualityError: If threshold is exceeded
        """
        if total_count == 0:
            return True
            
        failure_rate = failed_count / total_count
        if failure_rate > threshold:
            error_msg = f"Data quality threshold exceeded for {check_name}: {failure_rate:.2%} > {threshold:.2%}"
            logger.error(error_msg)
            raise DataQualityError(
                message=error_msg,
                file_name=file_name,
                quality_check=check_name,
                failed_count=failed_count,
                total_count=total_count,
                threshold=threshold
            )
        return True
