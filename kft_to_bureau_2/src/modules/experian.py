import sys
from pathlib import Path
from ..utils.utilities import S3Connector
from .. import session_ktb
from .. import config_ktb, logger_ktb
from snowflake.snowpark.functions import col
from datetime import date, datetime

class ExperianData:
    def __init__(self, config_ktb, s3_client):
        self.s3 = s3_client
        self.config_ktb = config_ktb
    
    def _get_next_bureau_id_index(self, date_prefix):
        """Get the next bureau ID index for a given date, continuing sequence if date is same"""
        query = f"""
        SELECT MAX(
            CAST(SUBSTR(BUREAU_ID, 16, 12) AS INTEGER)
        ) as max_index
        FROM EMBED_DB_TEST.BUREAU_DATA_EXCHANGE.EXPERIAN_DATA
        WHERE BUREAU_ID LIKE 'EX{date_prefix}%'
        """
        result = session_ktb.sql(query).collect()
        max_index = result[0][0] if result and result[0][0] else 0
        return max_index + 1
        
    def shortlist_exp_data(self, folder, input_score=600):
        query = f"""
            SELECT ed.KFT_ID, ed.BUREAU_ID, ed.HASHED_PHONE, ed.PHONE, ed.PAN_NO, ed.SCRUB_BATCH_NO, ed.SCRUB_DATE
            FROM embed_db_test.bureau_data_exchange.equifax_data ed
            LEFT JOIN embed_db_test.equifax_scrub.AR ar
                ON ed.bureau_id = ar.reference_no
            WHERE ed.scrub_batch_no = '{folder}'
            AND (ar.score >= {input_score})
            AND KFT_ID IS NOT NULL
        """
        return session_ktb.sql(query) 

    def send_shortlisted_data(self, folder, input_score=600):
        """Send shortlisted data to Experian using score filtering."""
        experian_data = self.shortlist_exp_data(folder, input_score)  
        
        saved = False
        from io import StringIO
        csv_buffer = StringIO()
        experian_data.to_pandas().to_csv(csv_buffer, index=False)
        
        bucket_name = self.config_ktb.location.bucket_name
        key = f"{self.config_ktb.location.folder_name}/experian/{folder}/{folder}_experian_data.csv"
        self.s3.put_object(Bucket=bucket_name, Key=key, Body=csv_buffer.getvalue())
        
        try:
            experian_data.write.save_as_table("EMBED_DB_TEST.BUREAU_DATA_EXCHANGE.EXPERIAN_DATA", mode="append")
            self.update_equifax_data()
            self.update_partners_data(folder)
        except Exception as e:
            logger_ktb.error(f"Error uploading data: {str(e)}")
            raise
    
    def send_all_data(self, batch_name):
        """Send all data from Equifax to Experian without filtering."""
        try:
            query = f"""
            SELECT KFT_ID, BUREAU_ID, HASHED_PHONE, PHONE, PAN_NO, SCRUB_BATCH_NO, SCRUB_DATE 
            FROM EMBED_DB_TEST.BUREAU_DATA_EXCHANGE.EQUIFAX_DATA
            WHERE SCRUB_BATCH_NO = '{batch_name}'
            """
            batch_data = session_ktb.sql(query)
            
            if batch_data.count() == 0:
                raise ValueError("No data found for this batch")
            
            # Save to S3
            from io import StringIO
            csv_buffer = StringIO()
            batch_data.to_pandas().to_csv(csv_buffer, index=False)
            
            bucket_name = self.config_ktb.location.bucket_name
            key = f"{self.config_ktb.location.folder_name}/experian/batch_{batch_name}/batch_{batch_name}_experian_data.csv"
            self.s3.put_object(Bucket=bucket_name, Key=key, Body=csv_buffer.getvalue())
            
            # Save to table
            batch_data.write.save_as_table(
                "EMBED_DB_TEST.BUREAU_DATA_EXCHANGE.EXPERIAN_DATA",
                mode="append"
            )
            
            # Update equifax_data to mark as sent to experian
            self.update_equifax_data()
            self.update_partners_data(batch_name)
            
        except Exception as e:
            logger_ktb.error(f"Error in send_all_data: {str(e)}")
            raise
         
    def update_equifax_data(self):
        update_query = """
            UPDATE EMBED_DB_TEST.BUREAU_DATA_EXCHANGE.EQUIFAX_DATA AS equifax
            SET equifax.SENT_TO_EXPERIAN = TRUE
            FROM EMBED_DB_TEST.BUREAU_DATA_EXCHANGE.EXPERIAN_DATA AS result
            WHERE equifax.KFT_ID = result.KFT_ID;
        """
        
        session_ktb.sql(update_query).collect()
    
    def process_partner_table_batch(self, dataframes_dict):
        """
        Process multiple partner batches as a single concatenated batch.
        
        Args:
            dataframes_dict: Dictionary with batch names as keys and dataframes as values
        
        Returns:
            Combined result dataframe
        """
        import snowflake.snowpark.functions as F
        from snowflake.snowpark.window import Window
        from io import StringIO

        scrub_batch = S3Connector(self.config_ktb).set_batch_number()
        partner_batches_list = [] 
        for batch_name, df in dataframes_dict.items():
            batch_count = df.count()
            partner_batches_list.append({
                "batch_name": batch_name,
                "record_count": batch_count
            })
        
        # Concatenate all dataframes
        combined_df = dataframes_dict[list(dataframes_dict.keys())[0]]
        for batch_name in list(dataframes_dict.keys())[1:]:
            combined_df = combined_df.union(dataframes_dict[batch_name])
        
        # Get current date prefix
        date_prefix = date.today().strftime('%d%m%Y')
        start_index = self._get_next_bureau_id_index(date_prefix)
        
        # Add BUREAU_ID with continuous indexing across all batches
        result = combined_df.with_column(
            "BUREAU_ID",
            F.concat(
                F.lit("EX"),
                F.lit(date_prefix),
                F.lpad(
                    F.row_number().over(Window.order_by(F.lit(1))) + F.lit(start_index - 1),
                    12,
                    F.lit('0')
                )
            )
        ).with_column("SCRUB_BATCH_NO", F.lit(scrub_batch)) \
         .with_column("SCRUB_DATE", F.current_date())
        
        # S3 export
        s3_data = result.select(col('BUREAU_ID'), col('NAME'), col('PHONE')).to_pandas()
        csv_buffer = StringIO()
        s3_data.to_csv(csv_buffer, index=False)
        
        bucket_name = self.config_ktb.location.bucket_name
        key = f"{self.config_ktb.location.folder_name}/experian/batch_{scrub_batch}/batch_{scrub_batch}_experian_data.csv"
        self.s3.put_object(Bucket=bucket_name, Key=key, Body=csv_buffer.getvalue())
        
        # Select required columns
        table_data = result.select(
            col('KFT_ID'), 
            col('BUREAU_ID'), 
            col('HASHED_PHONE'), 
            col('PHONE'), 
            col('PAN_NO'), 
            col('SCRUB_BATCH_NO'), 
            col('SCRUB_DATE')
        )
        
       

        try:    
            table_data.write.save_as_table("EMBED_DB_TEST.BUREAU_DATA_EXCHANGE.EXPERIAN_DATA", mode="append")
            logger_ktb.info("Batch data ingested to Experian successfully")
            logger_ktb.info(f"Scrub_Batch_no: {scrub_batch}")
            logger_ktb.info(f"Total records processed: {result.count()}")
            self.update_partners_data(scrub_batch)
            # S3Connector.save_analytics(
            #     json_data={
            #         "scrub_batch_no": scrub_batch,
            #         "partner_batches": partner_batches_list,
            #         "total_records_sent_to_experian": result.count(),
            #         "status": "sent to experian",
            #         "timestamp": datetime.now().isoformat()
            #         },
            #     filename=f"scrub_batch_{scrub_batch}_experian.json"
            # )
        except Exception as e:
            logger_ktb.error(f"Experian data ingestion failed: {str(e)}")
            raise
            
        return result

    def update_partners_data(self, scrub_batch):
        update_query = f"""
            UPDATE EMBED_DB_TEST.PARTNERS_DATA.PARTNERS_PROCESSED_DATA AS partners
            SET
                partners.SENT_TO_SCRUB = TRUE,
                partners.SENT_TO_EXPERIAN = TRUE,
                partners.SCRUB_DATE = CURRENT_DATE()
            FROM EMBED_DB_TEST.BUREAU_DATA_EXCHANGE.EXPERIAN_DATA AS result
            WHERE partners.KFT_ID = result.KFT_ID
            AND result.SCRUB_BATCH_NO = '{scrub_batch}';
        """
        session_ktb.sql(update_query).collect()
