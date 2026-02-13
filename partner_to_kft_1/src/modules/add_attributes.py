from src.modules import session_ptk
from src import logger_ptk, config_ptk
from snowflake.snowpark.functions import col

class DataPreparation:
    
    def match_phone(self, batch_name, partner_name):
        logger_ptk.info(f"Starting KFT ID matching for {batch_name} - {partner_name}")
        try:
            # Fetch only records that HAVE a KFT ID match
            query = f"""
                SELECT 
                    t.*, 
                    p.KFT_ID AS KFT_ID
                FROM embed_db_test.partners_data.partners_raw_data t
                INNER JOIN embed_db_test.KFT_PHONE_BASE.PHONE p
                    ON t.HASHED_PHONE = p.HASHED_PHONE
                WHERE t.batch_no = '{batch_name}'
                AND t.partner_name = '{partner_name}'
                AND t.status = 'not processed'
                """
            
            final_df = session_ptk.sql(query)
            return final_df
        except Exception as e:
            logger_ptk.error(f"Exception occurred while matching phone numbers: {e}")
            raise

    def mark_missing_kft_ids(self, batch_name, partner_name):
        """
        Mark records in raw data as error if no match found in KFT_PHONE_BASE.PHONE
        """
        logger_ptk.info(f"Marking records with missing KFT IDs for {batch_name} - {partner_name}")
        try:
            # Identify records where HASHED_PHONE doesn't exist in PHONE table
            # Optimized with NOT EXISTS
            query = f"""
                UPDATE embed_db_test.partners_data.partners_raw_data t
                SET status = 'error: kft id not found'
                WHERE t.batch_no = '{batch_name}'
                AND t.partner_name = '{partner_name}'
                AND t.status = 'not processed'
                AND (
                    t.HASHED_PHONE IS NULL 
                    OR NOT EXISTS (
                        SELECT 1 FROM embed_db_test.KFT_PHONE_BASE.PHONE p
                        WHERE p.HASHED_PHONE = t.HASHED_PHONE
                    )
                )
            """
            result = session_ptk.sql(query).collect()
            count = result[0][0] if result else 0
            logger_ptk.info(f"Successfully marked {count} missing KFT IDs as errors")
            return count
        except Exception as e:
            logger_ptk.error(f"Exception occurred while marking missing KFT IDs: {e}")
            raise
    
    def update_status(self, batch_name, partner_name):
        """
        Update raw data status to 'processed' for records successfully moved to PROCESSED table
        """
        try:
            # We update status to 'processed' for those that are in processed table
            # matching by reference ID, batch and partner
            # Using subquery instead of JOIN for better reliability in Snowflake UPDATEs
            query = f"""
                UPDATE embed_db_test.partners_data.partners_raw_data
                SET STATUS = 'processed'
                WHERE BATCH_NO = '{batch_name}'
                AND PARTNER_NAME = '{partner_name}'
                AND STATUS = 'not processed'
                AND PARTNER_REFERENCE_ID IN (
                    SELECT PARTNER_REFERENCE_ID 
                    FROM embed_db_test.partners_data.partners_processed_data
                    WHERE BATCH_NO = '{batch_name}'
                    AND PARTNER_NAME = '{partner_name}'
                )
            """
        
            session_ptk.sql(query).collect()
            logger_ptk.info(f"Updated raw data status to processed for {batch_name} - {partner_name}")
        except Exception as e:
            logger_ptk.error(f"Exception occurred while updating status: {e}")
            raise