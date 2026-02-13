"""
Custom exception classes for the S3 to Snowflake ETL pipeline.

This module provides specific exception types for different failure scenarios
in the pipeline, enabling better error handling and troubleshooting.
"""

from typing import Optional, Dict, Any, List
from ...src import logger_btk
logger = logger_btk



class PipelineException(Exception):
    """Base exception for all pipeline errors."""
    
    def __init__(
        self,
        message: str,
        error_code: str,
        context: Optional[Dict[str, Any]] = None,
        resolution: Optional[str] = None
    ):
        """
        Initialize pipeline exception.
        
        Args:
            message: Detailed error message
            error_code: Unique error code for cataloging
            context: Additional context information (file, row, column, etc.)
            resolution: Suggested resolution steps
        """
        self.message = message
        self.error_code = error_code
        self.context = context or {}
        self.resolution = resolution
        
        # Build comprehensive error message
        full_message = f"[{error_code}] {message}"
        
        if self.context:
            context_str = ", ".join([f"{k}={v}" for k, v in self.context.items()])
            full_message += f"\nContext: {context_str}"
        
        if self.resolution:
            full_message += f"\nResolution: {resolution}"
            logger.error(full_message)
        
        super().__init__(full_message)
        
  
        

class S3ConnectionError(PipelineException):
    """Exception raised for S3 connection and access errors."""
    
    def __init__(
        self,
        message: str,
        bucket: Optional[str] = None,
        key: Optional[str] = None,
        original_error: Optional[Exception] = None
    ):
        context = {}
        if bucket:
            context["bucket"] = bucket
        if key:
            context["key"] = key
        if original_error:
            context["original_error"] = str(original_error)
        
        resolution = (
            "1. Verify AWS credentials are correct\n"
            "2. Check bucket name and region\n"
            "3. Verify IAM permissions for S3 access\n"
            "4. Check network connectivity"
        )
        
        
        super().__init__(
            message=message,
            error_code="S3_001",
            context=context,
            resolution=resolution
        )


class SnowflakeConnectionError(PipelineException):
    """Exception raised for Snowflake connection errors."""
    
    def __init__(
        self,
        message: str,
        account: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        original_error: Optional[Exception] = None
    ):
        context = {}
        if account:
            context["account"] = account
        if database:
            context["database"] = database
        if schema:
            context["schema"] = schema
        if original_error:
            context["original_error"] = str(original_error)
        
        resolution = (
            "1. Verify Snowflake credentials\n"
            "2. Check account, database, and schema names\n"
            "3. Verify network connectivity to Snowflake\n"
            "4. Check warehouse status"
        )
        
        
        
        super().__init__(
            message=message,
            error_code="SF_001",
            context=context,
            resolution=resolution
        )


class SchemaValidationError(PipelineException):
    """Exception raised for schema validation errors."""
    
    def __init__(
        self,
        message: str,
        file_name: Optional[str] = None,
        expected_schema: Optional[str] = None,
        actual_schema: Optional[str] = None,
        missing_columns: Optional[list] = None,
        extra_columns: Optional[list] = None
    ):
        context = {}
        if file_name:
            context["file_name"] = file_name
        if expected_schema:
            context["expected_schema"] = expected_schema
        if actual_schema:
            context["actual_schema"] = actual_schema
        if missing_columns:
            context["missing_columns"] = ", ".join(missing_columns)
        if extra_columns:
            context["extra_columns"] = ", ".join(extra_columns)
        
        resolution = (
            "1. Verify file format matches expected schema\n"
            "2. Check for missing or renamed columns\n"
            "3. Validate file source and generation process\n"
            "4. Review schema definition in models"
        )
        
       
        
        super().__init__(
            message=message,
            error_code="SCHEMA_001",
            context=context,
            resolution=resolution
        )


class DataValidationError(PipelineException):
    """Exception raised for data validation errors."""
    
    def __init__(
        self,
        message: str,
        file_name: Optional[str] = None,
        row_number: Optional[int] = None,
        column_name: Optional[str] = None,
        invalid_value: Optional[Any] = None,
        expected_type: Optional[str] = None,
        validation_rule: Optional[str] = None
    ):
        context = {}
        if file_name:
            context["file_name"] = file_name
        if row_number is not None:
            context["row_number"] = row_number
        if column_name:
            context["column_name"] = column_name
        if invalid_value is not None:
            context["invalid_value"] = str(invalid_value)
        if expected_type:
            context["expected_type"] = expected_type
        if validation_rule:
            context["validation_rule"] = validation_rule
        
        resolution = (
            "1. Review the invalid value in the source file\n"
            "2. Check data generation process\n"
            "3. Verify data type compatibility\n"
            "4. Consider data cleansing or transformation"
        )
        
        super().__init__(
            message=message,
            error_code="DATA_001",
            context=context,
            resolution=resolution
        )


class FileFormatError(PipelineException):
    """Exception raised for file format and parsing errors."""

    def __init__(
        self,
        message: str,
        file_name: Optional[str] = None,
        expected_format: Optional[List[str]] = None,  
        delimiter: Optional[str] = None,
        encoding: Optional[str] = None,
        line_number: Optional[int] = None
    ):
        context = {}
        if file_name:
            context["file_name"] = file_name
        if expected_format:
            context["expected_format"] = ', '.join(expected_format)  # Join list to string
        if delimiter:
            context["delimiter"] = delimiter
        if encoding:
            context["encoding"] = encoding
        if line_number is not None:
            context["line_number"] = line_number

        resolution = (
            "1. Verify file delimiter and format\n"
            "2. Check file encoding (UTF-8 expected)\n"
            "3. Validate file is not corrupted\n"
            "4. Review file generation process"
        )

        super().__init__(
            message=message,
            error_code="FORMAT_001",
            context=context,
            resolution=resolution
        )



class ConfigurationError(PipelineException):
    """Exception raised for configuration validation errors."""
    
    def __init__(
        self,
        message: str,
        config_key: Optional[str] = None,
        config_value: Optional[Any] = None,
        expected_type: Optional[str] = None
    ):
        context = {}
        if config_key:
            context["config_key"] = config_key
        if config_value is not None:
            context["config_value"] = str(config_value)
        if expected_type:
            context["expected_type"] = expected_type
        
        resolution = (
            "1. Review configuration file (config.yaml)\n"
            "2. Verify all required parameters are set\n"
            "3. Check environment variables\n"
            "4. Validate configuration value types"
        )
        
        super().__init__(
            message=message,
            error_code="CONFIG_001",
            context=context,
            resolution=resolution
        )


class DataQualityError(PipelineException):
    """Exception raised for data quality issues."""
    
    def __init__(
        self,
        message: str,
        file_name: Optional[str] = None,
        quality_check: Optional[str] = None,
        failed_count: Optional[int] = None,
        total_count: Optional[int] = None,
        threshold: Optional[float] = None
    ):
        context = {}
        if file_name:
            context["file_name"] = file_name
        if quality_check:
            context["quality_check"] = quality_check
        if failed_count is not None:
            context["failed_count"] = failed_count
        if total_count is not None:
            context["total_count"] = total_count
        if threshold is not None:
            context["threshold"] = threshold
        
        resolution = (
            "1. Review data quality metrics\n"
            "2. Investigate source data issues\n"
            "3. Consider adjusting quality thresholds\n"
            "4. Implement data cleansing rules"
        )
        
        super().__init__(
            message=message,
            error_code="QUALITY_001",
            context=context,
            resolution=resolution
        )
