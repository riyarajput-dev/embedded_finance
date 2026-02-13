from typing import Dict, List, Optional, Any, Tuple
from snowflake.snowpark.types import IntegerType, StringType, StructType
from snowflake.snowpark import DataFrame
from ....src import aws_credentials, logger_btk
from ..pipeline import (
    save_table,
    session_btk,
    date_cols,
    amount_cols,
    experian_read_format,
    equifax_read_format,
)
import re
from ...utils.exceptions import SchemaValidationError, FileFormatError
from ...utils.validators import SchemaValidator, FileFormatValidator



logger = logger_btk
class ReadFile:
    def __init__(self, config):
        self.config = config
        self.read_logs=[]
        
    def read_file(
        self, schema: StructType, file_name: str, table_name: str
    ) -> Optional[DataFrame]:
        """Read file and return DataFrame"""
        logger.info(f"[{table_name}] -> Reading file {file_name}...")

        if re.search(r"experian", self.config.name):
            read_format = experian_read_format.copy()
            formt = ".txt"
        else:
            read_format = equifax_read_format.copy()
            formt = ".txt"       

        if table_name == "REFERENCE":
            read_format = {"SKIP_HEADER": 1, "FIELD_DELIMITER": ","}
            formt = ".csv"
            
        expected_format = [".csv", ".xlsx", ".txt"]
        is_valid = FileFormatValidator.validate_file_format(file_name, expected_format)
        
        if not is_valid:
            raise FileFormatError(
                message=f"{file_name}'s format is not valid.",
                file_name = file_name,
                expected_format = expected_format
            )
        
        df = (
            session_btk.read.options(read_format)
            .schema(schema)
            .csv(f"@{self.config.location.stage_name}/{file_name}")
        )
        
        count = df.count()
        self.read_logs.append(f"Found {count} records in {file_name} ")
        failed_files = {}
        if count==0 :
            actual_col = [col.name for col in df.schema.fields()]
            is_valid, missing_columns, extra_columns = SchemaValidator.validate_schema(actual_col,schema,file_name)
            
            if not is_valid:
                logger.error(f"Schema validation failed for file {file_name}")
                failed_files[file_name] = f"Schema mismatched for file name {file_name}"
                raise SchemaValidationError(
                    message=f"Schema validation failed for file {file_name}",
                    file_name=file_name,
                    expected_schema=", ".join([field.name for field in schema.fields]),
                    actual_schema=", ".join(actual_col),
                    missing_columns=missing_columns,
                    extra_columns=extra_columns
                )
            else:                
                logger.info(f"File {file_name} has no records.")
                self.read_logs.append(f"{file_name} has no records.")
                failed_files[file_name] = f"No records found for filename {file_name}"            
        else:
            logger.info("File read successfully")

        return df,count, failed_files, self.read_logs
       
       

            
