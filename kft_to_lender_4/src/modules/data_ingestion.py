import json
from datetime import datetime
from pathlib import Path
from .. import config_ktl, logger_ktl
from ..modules import session_ktl, save_table
from ..utils.stage_creator import StageCreator
from ..utils.utilities import StorageConnector
from snowflake.snowpark.functions import col, lit, sql_expr

logger = logger_ktl


class IngestData:
    def __init__(self, config):
        self.config = config
        self.session = session_ktl
        self.s3_client = StorageConnector(config).s3

    def fetch_shortlisting_criteria(self, lender_name, bureau_type):
        """Load shortlisting criteria from local config file."""
        config_path = Path(f"config/lenders/{lender_name.lower()}_{bureau_type.lower()}.json")
        
        if not config_path.exists():
            logger.warning(f"No config found at {config_path}")
            return None
            
        try:
            with open(config_path, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error reading config: {e}")
            return None

    def save_to_s3(self, df, bucket_name, key):
        """Save DataFrame to S3 as pipe-delimited CSV."""
        try:
            pdf = df.to_pandas()
            csv_buffer = pdf.to_csv(index=False, sep="|")
            self.s3_client.put_object(
                Bucket=bucket_name, 
                Key=key, 
                Body=csv_buffer.encode('utf-8')
            )
            logger.info(f"Saved to S3: s3://{bucket_name}/{key}")
            return True
        except Exception as e:
            logger.error(f"Failed to save to S3: {e}")
            return False

    def process_shortlisting(self, batch_name, lender_name, bureau_types):
        """
        Main shortlisting logic:
        1. Read data from S3: lenders-data-exchange/kft-to-lender/{lender_name}/{batch_name}/
        2. Find score file and apply threshold
        3. Filter all batch files to keep only shortlisted users
        4. Save results to lenders-data-exchange/kft-to-lender/shortlisted/{lender_name}/{batch_name}/
        5. Update lenders_shortlisted table
        """
        target_table = f"{self.config.database}.{self.config.schema}.lenders_shortlisted"
        bucket_name = self.config.location.bucket_name
        
        for bureau_type in bureau_types:
            logger.info(f"Processing: {lender_name} | {bureau_type}")
            
            # Create Snowflake stage
            stage_name = StageCreator(self.config).create_stage(lender_name, bureau_type)
            
            # Get shortlisting criteria
            criteria = self.fetch_shortlisting_criteria(lender_name, bureau_type)
            if not criteria:
                logger.warning(f"No criteria for {lender_name} | {bureau_type}")
                continue
            
            score_rule = next(
                (rule for rule in criteria.get("rules", []) 
                 if rule['field'].upper() in ["SCORE", "SCORE_V3"]), 
                None
            )
            
            if not score_rule:
                logger.warning(f"No SCORE rule in config")
                continue
            
            score_threshold = float(score_rule.get("value", 0))
            score_operator = score_rule.get("operator", ">=")
            logger.info(f"Score filter: {score_rule['field']} {score_operator} {score_threshold}")
            
            is_experian = bureau_type.upper() == "EXPERIAN"
            is_equifax = bureau_type.upper() == "EQUIFAX"
            
            if (lender_name.lower() in ["prefr", "zype"]) and is_experian:
                score_file_pattern = "SCORE"
                score_column = "SCORE_V3"
                id_column = "CUSTOMER_ID"
            elif lender_name.lower() == "zype" and is_equifax:
                score_file_pattern = "AR"
                score_column = "SCORE"
                id_column = "CUSTOMER_ID"
            else:
                logger.warning(f"Unsupported combination: {lender_name} | {bureau_type}")
                continue
            
            # Build S3 prefix
            source_prefix = f"kft-to-lender/{lender_name}/{batch_name}/"
            target_prefix = f"kft-to-lender/shortlisted/{lender_name}/{batch_name}/"
            logger.info(f"S3 source prefix: s3://{bucket_name}/{source_prefix}")
            
            # List files in batch
            response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=source_prefix)
            if 'Contents' not in response:
                continue
            
            all_files = [
                obj['Key'] for obj in response['Contents'] 
                if not obj['Key'].endswith('/') and "shortlisted" not in obj['Key']
            ]
            logger.info(f"Found {len(all_files)} files in batch")
            
            # Find score file
            score_files = [f for f in all_files if score_file_pattern in f.upper()]
            if not score_files:
                logger.warning(f"No {score_file_pattern} file found")
                continue
            
            score_file = score_files[0]
            score_file_name = score_file.split('/')[-1]
            logger.info(f"Using score file: {score_file_name}")
            
            # Read score file
            score_df = self.session.read \
                .option("PARSE_HEADER", True) \
                .option("FIELD_DELIMITER", "|") \
                .csv(f"@{stage_name}/{batch_name}/{score_file_name}")
            
            # Apply score filter
            score_df = score_df.with_column(score_column, col(score_column).cast("float"))
            
            if score_operator == ">=":
                filtered_df = score_df.filter(col(score_column) >= score_threshold)
            elif score_operator == "<=":
                filtered_df = score_df.filter(col(score_column) <= score_threshold)
            elif score_operator == ">":
                filtered_df = score_df.filter(col(score_column) > score_threshold)
            elif score_operator == "<":
                filtered_df = score_df.filter(col(score_column) < score_threshold)
            elif score_operator in ["=", "==", "==="]:
                filtered_df = score_df.filter(col(score_column) == score_threshold)
            else:
                logger.warning(f"Unsupported operator: {score_operator}")
                continue
            
            shortlisted_count = filtered_df.count()
            logger.info(f"Shortlisted {shortlisted_count} records")
            
            if shortlisted_count == 0:
                logger.info(f"No records shortlisted")
                continue
            
            # Create temp table with shortlisted IDs (memory efficient)
            temp_table = f"{self.config.database}.{self.config.schema}.temp_shortlisted_{lender_name}_{bureau_type}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            shortlisted_ids = filtered_df.select(col(id_column).alias("ID")).distinct()
            shortlisted_ids.write.mode("overwrite").save_as_table(temp_table, table_type="temporary")
            logger.info(f"Created temp table: {temp_table}")
            
            # Process all files and filter by shortlisted IDs
            temp_ids_df = self.session.table(temp_table)
            
            for file_path in all_files:
                file_name = file_path.split('/')[-1]
                
                # Read file
                df = self.session.read \
                    .option("PARSE_HEADER", True) \
                    .option("FIELD_DELIMITER", "|") \
                    .csv(f"@{stage_name}/{batch_name}/{file_name}")
                
                # Determine ID column for this file
                file_id_col = id_column
                
                # Join with shortlisted IDs
                filtered_file_df = df.join(
                    temp_ids_df, 
                    df[file_id_col] == temp_ids_df["ID"], 
                    "inner"
                )
                
                # Select only original columns
                filtered_file_df = filtered_file_df.select([df[c] for c in df.columns])
                
                row_count = filtered_file_df.count()
                
                if row_count > 0:
                    # Save to S3
                    s3_key = f"{target_prefix}{file_name}"
                    self.save_to_s3(filtered_file_df, bucket_name, s3_key)
                    
                    # Update metadata table (for main file only)
                    if score_file_pattern in file_name.upper():
                        metadata_df = filtered_file_df.select(
                            lit(datetime.now().date()).alias("CREATED_AT"),
                            sql_expr(f"substring({file_id_col}, 5)").alias("BUREAU_ID"),
                            col(file_id_col).alias("LENDER_UID"),
                            lit(batch_name).alias("LENDER_BATCH"),
                            lit(lender_name).alias("LENDER_NAME"),
                            lit(bureau_type).alias("BUREAU_TYPE")
                        )
                        
                        save_table(metadata_df, target_table, mode="append")
                        logger.info(f"Saved {row_count} records to {target_table}")
                
                logger.info(f"Processed {file_name}: {row_count} shortlisted records")
            
            # Cleanup temp table
            try:
                self.session.sql(f"DROP TABLE IF EXISTS {temp_table}").collect()
                logger.info(f"Dropped temp table: {temp_table}")
            except Exception as e:
                logger.warning(f"Failed to drop temp table: {e}")