from pathlib import Path
import sys
from .. import config_ktb
from .. import snowflake_connection_obj, save_table
from .. import logger_ktb
from snowflake.snowpark.functions import col, to_char, current_date
from datetime import datetime, timedelta
from ..utils.utilities import S3Connector
import snowflake.snowpark.functions as F
from snowflake.snowpark import Row
from snowflake.snowpark import WhenMatchedClause, WhenNotMatchedClause
import io
from .. import session_ktb
from datetime import date
    
class PartnerData:
    def __init__(self,config_ktb, s3_client):
        self.config_ktb = config_ktb
        self.s3 = s3_client
        
    def shortlist_data(self, days_threshold=90):
        query = f"""
        SELECT * FROM EMBED_DB_TEST.PARTNERS_DATA.PARTNERS_PROCESSED_DATA
        WHERE (SENT_TO_SCRUB = FALSE OR SCRUB_DATE < DATEADD(day, -{days_threshold}, CURRENT_DATE()))
        AND KFT_ID IS NOT NULL
        """
        return session_ktb.sql(query)
    
    def _get_next_bureau_id_index(self, date_prefix):
        """Get the next bureau ID index for a given date, continuing sequence if date is same"""
        query = f"""
        SELECT MAX(
            CAST(SUBSTR(BUREAU_ID, 16, 12) AS INTEGER)
        ) as max_index
        FROM EMBED_DB_TEST.BUREAU_DATA_EXCHANGE.EQUIFAX_DATA
        WHERE BUREAU_ID LIKE 'EQ{date_prefix}%'
        """
        result = session_ktb.sql(query).collect()
        max_index = result[0][0] if result and result[0][0] else 0
        return max_index + 1
    
    
    def process_partner_table_batch(self, dataframes_dict):
        """
        Process multiple partner batches as a single concatenated batch.
        
        Args:
            dataframes_dict: Dictionary with batch names as keys and dataframes as values
        
        Returns:
            Combined result dataframe
        """
        from snowflake.snowpark import DataFrame
        
        scrub_batch = S3Connector(config_ktb).set_batch_number()
        
        # Get shortlisted data count from shortlist_data query
        shortlisted_df = self.shortlist_data()
        shortlisted_count = shortlisted_df.count()
        
        # Collect partner batch details
        partner_batches_list = []
        
        # Concatenate all dataframes
        combined_df = dataframes_dict[list(dataframes_dict.keys())[0]]
        for batch_name in list(dataframes_dict.keys())[1:]:
            combined_df = combined_df.union(dataframes_dict[batch_name])
        
        # Build partner batches list with counts
        for batch_name, df in dataframes_dict.items():
            batch_count = df.count()
            partner_batches_list.append({
                "batch_name": batch_name,
                "record_count": batch_count
            })
        
        # Get current date prefix
        date_prefix = date.today().strftime('%d%m%Y')
        start_index = self._get_next_bureau_id_index(date_prefix)
        
        # Add BUREAU_ID with continuous indexing across all batches
        from snowflake.snowpark.window import Window 
        result = combined_df.with_column(
            "BUREAU_ID",
            F.concat(
                F.lit("EQ"),
                F.lit(date_prefix),
                F.lpad(
                    F.row_number().over(Window.order_by(F.lit(1))) + F.lit(start_index - 1),
                    12,
                    F.lit('0')
                )
            )
        ).with_column("SCRUB_BATCH_NO", F.lit(scrub_batch)) \
         .with_column("SCRUB_DATE", F.current_date()) \
         .with_column("SENT_TO_EXPERIAN", F.lit(False))
        
        # S3 export
        s3_data = result.select(col('BUREAU_ID'), col('NAME'), col('PHONE')).to_pandas()
        from io import StringIO
        csv_buffer = StringIO()
        s3_data.to_csv(csv_buffer, index=False)
        
        bucket_name = self.config_ktb.location.bucket_name
        key = f"{self.config_ktb.location.folder_name}/{self.config_ktb.location.object_name}/batch_{scrub_batch}/batch_{scrub_batch}_equifax_data.csv"
        self.s3.put_object(Bucket=bucket_name, Key=key, Body=csv_buffer.getvalue())
        
        # Select required columns
        table_data = result.select(
            col('KFT_ID'), 
            col('BUREAU_ID'), 
            col('HASHED_PHONE'), 
            col('PHONE'), 
            col('PAN_NO'), 
            col('SCRUB_BATCH_NO'), 
            col('SCRUB_DATE'), 
            col('SENT_TO_EXPERIAN')
        )

    
        try:
            table_data.write.save_as_table("embed_db_test.bureau_data_exchange.equifax_data", mode="append")
            logger_ktb.info("Batch data ingested successfully")
            logger_ktb.info(f"Scrub_Batch_no: {scrub_batch}")
            logger_ktb.info(f"Total records processed: {result.count()}")
            
            # Save analytics with partner batches and shortlisted data count
            # from src.utils.utilities import StorageConnector
            # analytics_data = {
            #     "scrub_batch_no": scrub_batch,
            #     "partner_batches": partner_batches_list,
            #     "shortlisted_data_count": shortlisted_count,
            #     "total_records_sent_to_equifax": result.count(),
            #     "status": "sent to equifax",
            #     "timestamp": datetime.now().isoformat()
            # }
            # S3Connector.save_analytics(
            #     json_data=analytics_data,
            #     filename=f"scrub_batch_{scrub_batch}_equifax.json"
            # )
            
            self.update_partners_data(scrub_batch)
        except Exception as e:
            logger_ktb.error(f"Data ingestion failed: {str(e)}")
            raise
        
        return result
    
    def update_partners_data(self, scrub_batch):
        update_query = f"""
            UPDATE EMBED_DB_TEST.PARTNERS_DATA.PARTNERS_PROCESSED_DATA AS partners
            SET
                partners.SENT_TO_SCRUB = TRUE,
                partners.SENT_TO_EQUIFAX = TRUE,
                partners.SCRUB_DATE = CURRENT_DATE()
            FROM EMBED_DB_TEST.BUREAU_DATA_EXCHANGE.EQUIFAX_DATA AS result
            WHERE partners.KFT_ID = result.KFT_ID
            AND result.SCRUB_BATCH_NO = '{scrub_batch}';
        
        """
        
        session_ktb.sql(update_query).collect()

   
    
        