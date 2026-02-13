from snowflake.snowpark.functions import expr
import pandas as pd
from snowflake.snowpark import DataFrame, Window
from models.partner_schema import file_schema
from snowflake.snowpark.types import StructType, StructField
from .file_reader import ReadFile
from .. import config_ptk
from ..modules import save_table, save_locally, session_ptk
from .. import logger_ptk
from .add_attributes import DataPreparation
from ..utils.utilities import StorageConnector
from datetime import datetime , timedelta
import os
from snowflake.snowpark.functions import col, count
import snowflake.snowpark.functions as F



class DataProcessor:
    def __init__(self, config_ptk):
        self.df: DataFrame = None
        self.schema: StructType = file_schema
        self.cleaning_errors = []
    
    def clean_data(self, df: DataFrame, batchname: str, partner_name: str):
       
        original_count = df.count()
        if original_count == 0:
            return df, 0, 0, 0

        logger_ptk.info(f"Starting cleaning for {partner_name} - {batchname}. Original count: {original_count}")

        try:
            final_df = DataPreparation().match_phone(batchname, partner_name)
        except Exception as e:
            logger_ptk.warning(f"Error during KFT ID matching: {e}")
                
        cleaned_count = final_df.count()
        duplicates = original_count - cleaned_count
        
        logger_ptk.info(f"Cleaning complete. Cleaned count: {cleaned_count}, Duplicates dropped: {duplicates}")
        
        return final_df, cleaned_count, original_count, duplicates
    

    def process_file(self, partner_name: str, batch_no: int, snow_df: DataFrame = None):
        """
        Process a specific batch for a partner from PARTNERS_RAW_DATA to PARTNERS_PROCESSED_DATA.
        1. Mark missing KFT IDs as errors.
        2. Fetch valid records with KFT ID mapping.
        3. Save to Processed Table.
        4. Update status in Raw Table.
        """
        logger_ptk.info(f"Processing batch {batch_no} for partner {partner_name} from Snowflake")
        
        try:
            batch_str = f"batch_{batch_no}"
            prep = DataPreparation()

            # Step 0: Get counts from raw for summary
            # Optimized: Single aggregation for total and not-processed counts
            raw_counts = session_ptk.table("PARTNERS_RAW_DATA").filter(
                (col("PARTNER_NAME") == partner_name) & (col("BATCH_NO") == batch_str)
            ).agg(
                F.count(F.lit(1)).alias("total"),
                F.sum(F.when(col("status") == "not processed", 1).otherwise(0)).alias("not_processed")
            ).collect()[0]

            total_original = raw_counts["TOTAL"] or 0
            not_processed_count = raw_counts["NOT_PROCESSED"] or 0
            
            # Duplicates defined as Original - records that reached raw as 'not processed'
            duplicates_at_ingestion = total_original - not_processed_count

            # Step 1: Mark records with no KFT ID match as errors in Raw Table
            missing_kft_id_count = prep.mark_missing_kft_ids(batch_str, partner_name)
            
            # Step 2: Fetch valid records (only those that have a KFT ID match)
            final_df = prep.match_phone(batch_str, partner_name)
            valid_for_processing = final_df.count() 
            
            if valid_for_processing == 0:
                logger_ptk.warning(f"No valid records (with KFT IDs) found for partner {partner_name} batch {batch_no}")
                return {
                    'status': 'success',
                    'message': f"No valid records to process. Errors marked in RAW table.",
                    'original_count': total_original,
                    'cleaned_count': 0,
                    'duplicates_found': duplicates_at_ingestion,
                    'missing_kft_id_count': missing_kft_id_count,
                    'batch_no': batch_no
                }

            # Step 3: Clean data (uniqueness, etc.)
            # Drop columns that will be re-added by save_table or are metadata from RAW
            final_df = final_df.drop("FILENAME", "STATUS", "CREATED_AT", "BATCH_NO", "PARTNER_NAME")
            cleaned_count = final_df.count()
            
            # Step 4: Save to Processed Table
            try:
                save_table(final_df, "PARTNERS_PROCESSED_DATA", batch_no, partner_name, "append")
                
                # Step 5: Update status in Raw Table for successfully ingested records
                prep.update_status(batch_str, partner_name)
                
            except Exception as e:
                logger_ptk.error(f"Error occurred during ingestion: {e}")
                raise
            
            logger_ptk.info(f"Successfully processed batch {batch_no} for {partner_name}")
            
            return {
                'status': 'success',
                'original_count': total_original,
                'cleaned_count': cleaned_count,
                'duplicates_found': duplicates_at_ingestion,
                'missing_kft_id_count': missing_kft_id_count,
                'batch_no': batch_no
            }
            
        except Exception as e:
            logger_ptk.error(f"Error processing batch {batch_no}: {str(e)}")
            return {
                'status': 'error',
                'message': str(e)
            }

    def get_pending_batches(self):
        """
        Get list of all batches from RAW data with their record counts and processing status.
        Returns a list of dicts: {'partner_name': str, 'batch_no': int, 'no_of_records': int, 'status': str}
        """
        try:
            # 1. Get all batches from Raw with record counts (total and new)
            raw_data = session_ptk.table("PARTNERS_RAW_DATA").group_by("partner_name", "batch_no").agg(
                F.count(F.lit(1)).alias("total_count"),
                F.sum(F.when(col("status") == "not processed", 1).otherwise(0)).alias("new_count")
            ).collect()
            
            # 2. Build status list
            all_batches = []
            for row in raw_data:
                p_name = row['PARTNER_NAME']
                b_str = row['BATCH_NO']
                total = row['TOTAL_COUNT']
                new_count = row['NEW_COUNT']
                
                try:
                    # b_str is "batch_123"
                    batch_num = int(b_str.replace("batch_", ""))
                    status = "processed" if new_count == 0 else "not processed"
                    
                    all_batches.append({
                        'partner_name': p_name,
                        'batch_no': batch_num,
                        'no_of_records': new_count if new_count > 0 else total, # Show 'new' count if any, else show total for context
                        'status': status
                    })
                except ValueError:
                    continue
            
            # Sort by partner then batch
            all_batches.sort(key=lambda x: (x['partner_name'], x['batch_no']))
            return all_batches
            
        except Exception as e:
            logger_ptk.error(f"Error getting batches with status: {e}")
            return []

      

    def get_files_for_batch(self, partner_name: str, batch_no: int):
        """
        Get list of files for a specific batch and partner with record counts and status.
        Returns a list of dicts: {'filename': str, 'no_of_records': int, 'status': str}
        """
        try:
            batch_str = f"batch_{batch_no}"
            
            # 1. Get files and their counts from RAW
            raw_files = session_ptk.table("PARTNERS_RAW_DATA").filter(
                (expr(f"partner_name = '{partner_name}'")) & 
                (expr(f"batch_no = '{batch_str}'"))
            ).group_by("filename").agg(
                F.count(F.lit(1)).alias("total_count"),
                F.sum(F.when(col("status") == "not processed", 1).otherwise(0)).alias("new_count")
            ).collect()
            
            files_info = []
            for row in raw_files:
                new_c = row['NEW_COUNT']
                status = "processed" if new_c == 0 else "not processed"
                files_info.append({
                    'filename': row['FILENAME'],
                    'no_of_records': new_c if new_c > 0 else row['TOTAL_COUNT'],
                    'status': status
                })
            
            return files_info
        except Exception as e:
            logger_ptk.error(f"Error getting files for batch {batch_no}: {e}")
            return []
        
        
    def get_data_cleaning_errors(self):
        """Return accumulated schema errors for UI display"""
        return self.cleaning_errors

   
