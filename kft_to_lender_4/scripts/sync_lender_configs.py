import os
import sys
import json
import logging
from pathlib import Path

# Add project root to sys.path
project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from ..src.modules import session_ktl
from ..src import config_ktl, logger_ktl
from snowflake.snowpark.functions import col

logger = logger_ktl
def sync_lender_configs():
    """
    Fetch criteria from embed_db_test.lenders_data.lenders_master
    and save them as local JSON files in config/lenders/
    """
    try:
        # Define paths
        config_dir = Path("config/lenders")
        config_dir.mkdir(parents=True, exist_ok=True)

        table_name = f"{config_ktl.database}.{config_ktl.schema}.lenders_master"
        logger.info(f"Fetching lender criteria from {table_name}...")

        # Query Snowflake
        df = session_ktl.table(table_name).select("lender_name", "bureau_type", "eligible_criteria")
        results = df.collect()

        if not results:
            logger.warning("No records found in lenders_master.")
            return

        for row in results:
            lender_name = row['LENDER_NAME']
            bureau_type = row['BUREAU_TYPE']
            criteria_json = row['ELIGIBLE_CRITERIA']

            if not criteria_json:
                logger.warning(f"No criteria found for {lender_name} | {bureau_type}. Skipping.")
                continue

            # Parse JSON if it's a string
            if isinstance(criteria_json, str):
                try:
                    criteria_data = json.loads(criteria_json)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON for {lender_name} | {bureau_type}: {e}")
                    continue
            else:
                criteria_data = criteria_json

            # Save to local file
            file_name = f"{lender_name.lower()}_{bureau_type.lower()}.json"
            file_path = config_dir / file_name

            with open(file_path, "w") as f:
                json.dump(criteria_data, f, indent=4)
            
            logger.info(f"Saved config: {file_path}")

        logger.info("Sync completed successfully.")

    except Exception as e:
        logger.error(f"Sync failed: {e}")

if __name__ == "__main__":
    sync_lender_configs()
