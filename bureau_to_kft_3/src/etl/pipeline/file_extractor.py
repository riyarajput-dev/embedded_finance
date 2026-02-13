import re
import boto3
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from ...utils.utilities import generate_ascii_art, lender_data_s3_push
from box import ConfigBox
from snowflake.snowpark import DataFrame
import snowflake.snowpark.functions as F
from snowflake.snowpark.functions import col, when, udf, to_date
from snowflake.snowpark.types import IntegerType, StringType, StructType
from snowflake.snowpark.window import Window
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd


from ....lender_scripts.zype_experian import mappings
from ....lender_scripts.zype_equifax import zype_eq_mappings
from ....lender_scripts.prefr import prefr_mappings
from ...etl.pipeline.storage_connector import StorageConnector
from ...etl.pipeline.file_parser import Parse
from ...etl.pipeline.file_reader import ReadFile
from ...etl.pipeline.processed_files_tracker import ProcessedFilesTracker
from ...utils.exceptions import SchemaValidationError, FileFormatError


from ....models.experian_models import (
    schema_for_address_file,
    schema_for_AR_file,
    schema_for_email_file,
    schema_for_ENQ_file,
    schema_for_id_file,
    schema_for_name_dob_file,
    schema_for_phone_file,
    schema_for_score_file,
    schema_for_reference_file,
    schema_for_employment_file,
    schema_for_fintech_enquiries_file,
    schema_for_propensity_score_file,
)

from ....models.equifax_models import (
    schema_for_pii_file,
    schema_for_ar_file,
    schema_for_enq_file,
)


from ...etl.pipeline import (
    save_table,
  
    session_btk,
    date_cols,
    amount_cols,
    experian_read_format,
    equifax_read_format,
)
from ...etl.pipeline.report_generator import summary, pivot_df, save
from ....src import aws_credentials, logger_btk
from ...utils.utilities import S3_loc



class FileExtractor:
    def __init__(self, config_btk: ConfigBox, max_workers: int = 3) -> None:
        self.data: DataFrame = None
        self.reference: DataFrame = None
        self.config = config_btk
        self.max_workers = max_workers
        self.count = 0
        self.tracker = ProcessedFilesTracker()  
        self.st_logs = []
     
        if re.search(r"experian", self.config.name):
            self.db_schemas = {
                # "REFERENCE": schema_for_reference_file,
                "ADDRESS": schema_for_address_file,
                "AR": schema_for_AR_file,
                "EMAIL": schema_for_email_file,
                "ENQ": schema_for_ENQ_file,
                "ID": schema_for_id_file,
                "NAME_DOB": schema_for_name_dob_file,
                "PHONE": schema_for_phone_file,
                "SCORE": schema_for_score_file,
                # "EMPLOYMENT": schema_for_employment_file,
                # "FINTECH_ENQUIRIES": schema_for_fintech_enquiries_file,
                # "PROPENSITY_SCORE": schema_for_propensity_score_file,
            }
        else:
            self.db_schemas = {
                "PII": schema_for_pii_file,
                "AR": schema_for_ar_file,
                "ENQ": schema_for_enq_file,
            }

        logger_btk.info("[FILE EXTRACTION] Initializing file extraction process.")

        generate_ascii_art("Data Loader")



    def extract_experian_files(
        self, table_name: str, identity_tag: str, file_name: str
    ) -> Optional[DataFrame]:
        """Extract and process Experian file, return DataFrame"""


        # Read file - returns DataFrame or None
        readfile = ReadFile(self.config)
        df, count, failed_files, read_logs = readfile.read_file(self.db_schemas.get(table_name), file_name, table_name)
        self.st_logs.append(read_logs)
      
        count = df.count() if df else 0

        if df is None:
            logger_btk.warning(f"⚠️  Skipping {table_name} - file not available")
            return None

        # Parse file
        parsefile = Parse()
        if file_name not in failed_files:
            try:
                df = parsefile.parse_file_df(df)
                logger_btk.info(f"[{table_name}] -> Parsing completed for {file_name}")
            except:
                logger_btk.warning(f"Parsing failed for {file_name}")
                failed_files.append(file_name)
                
        

        # Handle specific file types
        if table_name in ["SCORE", "PROPENSITY_SCORE"]:
            df = df.withColumnRenamed("CUST_ID", "CUSTOMER_ID")
            logger_btk.info(
                f"[{table_name}] -> Renamed CUST_ID to CUSTOMER_ID for {file_name}"
            )

        elif table_name == "EMAIL":
            email_columns = [c for c in df.columns if "EMAIL" in c.upper()]
            for c in email_columns:
                df = df.withColumn(c, F.lower(F.col(c)))
                df = df.withColumn(c, F.regexp_replace(F.col(c), r"\\+", ""))
                df = df.withColumn(c, F.regexp_replace(F.col(c), r'"+', ""))
                df = df.withColumn(c, F.regexp_replace(F.col(c), r"\s+", ""))
                df = df.withColumn(c, F.regexp_replace(F.col(c), r"[;:,]+$", ""))
                df = df.withColumn(
                    c,
                    F.regexp_replace(
                        F.col(c), r"(@gmail)\.(com|con|co)$", "@gmail.com"
                    ),
                )

        elif table_name == "REFERENCE":
            df = df.withColumnRenamed("MEMBER_REFERENCE", "CUSTOMER_ID")
            pattern = r".*PHONE_NUMBER.*"
            phone_number_columns = [
                col_name
                for col_name in df.columns
                if re.match(pattern, col_name, re.IGNORECASE)
            ]

            if len(phone_number_columns) == 0:
                raise ValueError("No PHONE_NUMBER columns found!")
            elif len(phone_number_columns) == 1:
                df = df.withColumnRenamed(phone_number_columns[0], "PHONE_NUMBER")
            else:
                main_phone_number_column = phone_number_columns[0]
                df = df.withColumnRenamed(main_phone_number_column, "PHONE_NUMBER")
                columns_to_drop = phone_number_columns[1:]
                df = df.drop(*columns_to_drop)

        # Remove null CUSTOMER_IDs and duplicates
        df = df.dropna(subset="CUSTOMER_ID")

        original_count = count
        df = df.dropDuplicates()
        final_count = df.count()

        duplicates_removed = original_count - final_count
        
                            
        if duplicates_removed > 0:
            logger_btk.info(
                f"[{table_name}] -> Data cleaning: Removed {duplicates_removed:,} duplicates, {final_count:,} records retained from {file_name}"
            )
            self.st_logs.append(f"Found {duplicates_removed} duplicate records in file {file_name}")
        else:
            logger_btk.info(
                f"[{table_name}] -> Data validation: No duplicates found in {file_name}"
            )

        return df, count, failed_files, self.st_logs


    def parse_columns(self, cols: list) -> dict:
        return {
            i: re.sub(
                r"_+",
                "_",
                re.sub(r"\"+|\'+|\/.*", "", re.sub(r"\s+|-", "_", str(i).strip())),
            )
            for i in cols
        } 
        
    
            
    def extract_equifax_files(self, table_name: str, identity_tag: str, file_name):
        # Try to read file - returns False if not found
        readfile = ReadFile(self.config)
        df, count, failed_files, read_logs = readfile.read_file(self.db_schemas.get(table_name), file_name, table_name)
        self.st_logs.append(read_logs)
      
        count = df.count() if df else 0

        if df is None:
            logger_btk.warning(f"⚠️  Skipping {table_name} processing - file not available")
            return None, 0, []  # Skip this file

        # parsing columns
        col_mapping = self.parse_columns(list(df.columns))
        df = df.select([col(c).alias(col_mapping.get(c, c)) for c in df.columns])
        # self.data.show(1)
        # self.parse_file()
        # self.data = self.data.with_columns("parsed_flag", F.lit(1))
        # self.data.show(1)
        
        parsefile = Parse()
        if file_name not in failed_files:
            try:
                df = parsefile.parse_file_df(df)
                logger_btk.info(f"[{table_name}] -> Parsing completed for {file_name}")
            except:
                logger_btk.warning(f"Parsing failed for {file_name}")
                failed_files.append(file_name)
        if table_name == 'ENQ' and 'INQ_TIME' in df.columns:
            df = df.withColumn(
                "INQ_TIME", 
                F.expr("TRY_TO_DATE(SPLIT_PART(INQ_TIME, ' ', 1), 'DD-MON-YY')")
            )
        # Remove duplicate rows
        original_count = df.count()
        df = df.dropDuplicates()
        final_count = df.count()

        duplicates_removed = original_count - final_count
        
        if duplicates_removed > 0:
            logger_btk.warning(f"⚠️  Removed duplicate rows from {table_name}")
            self.st_logs.append(f"Found {duplicates_removed} duplicates from {file_name}")
            logger_btk.info(f"Final record count: {final_count:,}")
        else:
            logger_btk.info(f"✓ No duplicates found in {table_name}")

        return df, count, failed_files, self.st_logs  # File processed successfully
    
    def determine_func_call(self, processed_df, table_name: str, identity_tag: str):
        bureau_name = self.config.name.lower()
        df = None
        folder_name = None
        
        if bureau_name == "experian":
            # zype_experian_map = mappings()
            # if table_name in zype_experian_map:
            #     func = zype_experian_map[table_name]
            #     df = func(session_btk, processed_df)
            #     folder_name = "zype_experian"
               
            prefr_map = prefr_mappings()
            if table_name in prefr_map:
                func = prefr_map[table_name]
                df = func(session_btk, processed_df)
                folder_name = "prefr" 
              
                
        else:
            zype_equifax_map = zype_eq_mappings()
            if table_name in zype_equifax_map:
                func = zype_equifax_map[table_name]
                df = func(session_btk, processed_df)
                folder_name = "zype_equifax"
             
                
        
        # Save dataframe as pipe-separated txt file and push to S3
        if df is not None and folder_name is not None:
            # Filter by identity_tag/batch_no to get only current batch data
            logger_btk.info(f"Pushing {table_name} data to S3 in folder {folder_name}...")
            lender_data_s3_push(
                df=df,
                batch_no=identity_tag,
                folder_name=folder_name,
                filename=table_name
            )
        

    def _process_single_file(
        self,
        table_name: str,
        schema: StructType,
        identity_tag: str,
        fun_call,
        file_name: str,
        batch_id: str,
    ) -> Tuple[str, int]:
        """Process a single file in isolation and return status and record count"""
        # Process file - returns DataFrame or None
        processed_df, count, failed_files, logs = fun_call(table_name, identity_tag, file_name)

        if processed_df is None:
            return (f"Skipped", 0, 0, [])
        
        
        original_count = count
        record_count = processed_df.count()
        # Save to Snowflake
        if file_name not in failed_files:
            self.determine_func_call(processed_df, table_name, identity_tag)
    
            save_table(processed_df, f"{table_name}", "append", batch_no=identity_tag)
            logger_btk.info(f"[{table_name}] -> saved successfully")
            
            
            logger_btk.info(f"upload to s3 successful for {file_name}")
            
            
            
            # Mark file as processed in tracker using S3 path
            self.tracker.mark_processed(batch_id, file_name, record_count)
        else:
            logger_btk.info(f"upload failed for {file_name}")
            failed_files.append(file_name)
            return (f"Failed",  record_count, original_count, failed_files)
        
        
        

        logger_btk.info(
            f"[{table_name}] -> Processed {file_name}: {record_count:,} records"
        )

        return (f"Processed", record_count, original_count, failed_files)

    def perform_extraction(self,  available_files, s3_path):
        """
        Perform extraction with parallel file processing.
        
        Args:
            files_to_process: Optional list of specific file names to process. 
                              If None, fetches all available files from S3.
        """
        logger_btk.info(f"\n{'-'*100}")
        logger_btk.info("[FILE EXTRACTION] Starting file extraction and processing")
        logger_btk.info(f"{'-'*100}")
        
        storage_connector = StorageConnector(self.config)
        
        # Use S3 path as unique batch identifier for tracking
        # We need the s3_path even if we have specific files, to check against tracker
        # But get_list_of_sets_available_in_s3 does listing which might be redundant if we have files
        # However, we need the s3_path for the batch_id.
        # Let's call it anyway to get the path, or construct it.
        # storage_connector.get_list_of_sets_available_in_s3 returns (list, path)
        
        available_files_s3, s3_path = storage_connector.get_list_of_sets_available_in_s3()
        
        # if files_to_process:
        #     available_files = files_to_process
        #     logger_btk.info(f"[FILE EXTRACTION] Processing specific files provided: {len(available_files)} files")
        # else:
        #     available_files = available_files_s3
        #     logger_btk.info(f"[FILE EXTRACTION] Files found in S3: {', '.join(available_files)}")

        
        # Use S3 path as unique batch identifier for tracking
        batch_id = s3_path
        identity_tag = self.config.location.get("identity_tag", f"{self.config.location.batch}")
        
        
        # Filter out already processed files using tracker with S3 path as batch ID
        # Even for manual selection, we might want to know if it was processed, 
        # but usually manual means "do it now". 
        # However, to be consistent and safe, let's filter, but maybe log a warning if a selected file is skipped?
        # For now, standard behavior: filter unprocessed.
        
        unprocessed_files, logs = self.tracker.filter_unprocessed(batch_id, available_files)
        
        if not unprocessed_files:
            logger_btk.info("[FILE EXTRACTION] All selected files have been processed already. Nothing to do.")
            logger_btk.info(f"{'-'*100}")
            self.st_logs.append(logs)

            
            return "All files processed"
        
        # Update available_files to only include unprocessed files
        available_files = unprocessed_files
        logger_btk.info(f"[FILE EXTRACTION] Files to process: {', '.join(available_files)}")
        self.st_logs.append(f"Processing {', '.join(available_files)} ")
        
        # Determine function to call based on the config (Experian or Equifax)
        if re.search(r"experian", self.config.name):    
            fun_call = self.extract_experian_files
            source = "EXPERIAN"
        else:
            fun_call = self.extract_equifax_files
            source = "EQUIFAX"

        # max_workers = min(len(self.db_schemas), 8)
        max_workers = self.max_workers
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            

            logger_btk.info(
                f"[FILE EXTRACTION] Found {len(available_files)} files to process"
            )
            logger_btk.info(
                f"[FILE EXTRACTION] Processing started with {max_workers} worker(s)..."
            )
            logger_btk.info(f"{'-'*80}")

            # Track futures for all file processing tasks
            future_to_file = {}
            file_table_mapping = {}  # Track which table each file maps to
            unmatched_files = []

            for file_name in available_files:
                table_name = None

                for tablename, schema in self.db_schemas.items():
                    pattern = rf".*" + re.escape(tablename.lower()) + ".*"
                    
                    if re.match(pattern, file_name.lower(), re.IGNORECASE):
                        table_name = tablename
                        break

                if table_name is None:
                    unmatched_files.append(file_name)
                    continue

                file_table_mapping[file_name] = table_name
                future = executor.submit(
                    self._process_single_file,
                    table_name,
                    schema,
                    identity_tag,
                    fun_call,
                    file_name,
                    batch_id,
                )
                future_to_file[future] = file_name

            files_processed = 0
            files_failed = 0
            total_records = 0
            processed_files = []
            failed = []

            result = []

            for future in as_completed(future_to_file):
                file_name = future_to_file[future]
                table_name = file_table_mapping.get(file_name, "UNKNOWN")
                try:
                    status, record_count, original_count, failed_files = future.result()
                    if status == "Processed":
                        files_processed += 1
                        total_records += record_count
                        processed_files.append(file_name)
                        result.append(
                            {
                                "FILE_NAME": file_name,
                                "BATCH": identity_tag,
                                "ORIGINAL_COUNT": original_count,
                                "FINAL_COUNT": record_count,
                                "DUPLICATES_AND_NULLS": original_count - record_count,
                                "STATUS": status,
                                "COUNT": 1,
                            }
                        )
                    else:
                        logger_btk.error(f"{file_name} failed")
                    
                        files_failed += 1
                        failed.append(file_name)
                        
                        
            

                except Exception as e:
                    logger_btk.error(f"exception occurred: {str(e)}")
                    failed.append(file_name)
                    if isinstance(e, (FileFormatError, SchemaValidationError)):
                        raise e
                        
            final_processed_files = self.tracker.get_processed_files(batch_id)
            
            logger_btk.info(f"available in s3 : {available_files_s3}")
            logger_btk.info(f"processed files: {final_processed_files}")
            
            if set(available_files_s3) ==  set(final_processed_files):
                final_status = "success"
            else:
                final_status = "failed"
                
            self.tracker.mark_final_status(batch_id,final_status)
                


            # result_df = pd.DataFrame(result)
            # init_report = pivot_df(result_df)
            # init_report.to_csv(r"reports/summary_rpt.csv", index=False, mode="w")

            # db_report = save()  
            # final_rpt = init_report.merge(
            #     db_report, on=["FILE_NAME", "BATCH"], how="left"
            # )
            # final_rpt.to_csv(r"reports/final_report.csv", mode="w", header=True, index=False)
            logger_btk.info(f"{'-'*80}")
            logger_btk.info(f"\n{'-'*100}")
            logger_btk.info(f"[FILE EXTRACTION] Processing Completed")
            
        
            logger_btk.info(f"{'-'*100}")
            logger_btk.info(f"Source: {source}")
            logger_btk.info(f"Files Found: {len(available_files)}")
            logger_btk.info(f"Files Processed: {files_processed}")
            logger_btk.info(f"Files Failed: {files_failed}")
            logger_btk.info(f"Files Unmatched: {len(unmatched_files)}")
            logger_btk.info(f"Total Records Loaded: {total_records:,}")

            if processed_files and len(processed_files) <= 10:
                logger_btk.info(f"\nProcessed Files:")
                for f in processed_files:
                    logger_btk.info(f"  ✓ {f}")
            elif processed_files:
                logger_btk.info(
                    f"\nProcessed {len(processed_files)} files ({total_records:,} total records)"
                )

            if unmatched_files:
                logger_btk.warning(f"\nUnmatched Files (No table mapping found):")
                for f in unmatched_files[:10]:
                    logger_btk.warning(f"  - {f}")
                    self.st_logs.append(f"Unmatched files found: {f}")
                if len(unmatched_files) > 10:
                    logger_btk.warning(f"  ... and {len(unmatched_files) - 10} more")


            if files_failed>0:
                logger_btk.error(f"\nFailed Files:")
                for f in failed:
                    logger_btk.error(f"  ✗ {f}")

           
        return "Data loaded successfully"

        # Summary
        # logger_btk.info(f"\n{'-'*100}")
        # logger_btk.info(f"EXTRACTION SUMMARY")
        # logger_btk.info(f"{'-'*100}")
        # logger_btk.info(f"Files processed: {files_processed}")
        # logger_btk.info(f"Files skipped: {files_skipped}")
        # logger_btk.info(f"Total files: {len(self.db_schemas)}")

        # if processed_files:
        #     logger_btk.info(f"Processed: {', '.join(processed_files)}")
        # if skipped_files:
        #     logger_btk.warning(f"Skipped: {', '.join(skipped_files)}")

        # return "data loaded successfully."
