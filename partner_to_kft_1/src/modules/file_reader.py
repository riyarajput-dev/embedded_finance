import os
from pathlib import Path
from typing import Tuple
from io import BytesIO
from datetime import datetime

import pandas as pd

from ...models.partner_schema import file_schema
from ..utils.utilities import StorageConnector
from .. import  logger_ptk, config_ptk
from ..modules import session_ptk, save_locally, save_raw_table
from snowflake.snowpark.functions import col, count




COLUMN_MAPPING = {
    "PId": "PARTNER_REFERENCE_ID",
    "pid": "PARTNER_REFERENCE_ID",
    "PID" : "PARTNER_REFERENCE_ID",
    "phone": "PHONE",
    "masked_phone": "MASKED_PHONE",
    "hashed_phone": "HASHED_PHONE",
}

REQUIRED_COLUMNS = ["PARTNER_REFERENCE_ID"]


class ReadFile:
    def __init__(self, config_ptk):
        self.config_ptk = config_ptk
        self.s3 = StorageConnector(self.config_ptk)

        # UI logs
        self.schema_errors = []
        self.duplicate_logs = []
    

    def validate_schema(self, df: pd.DataFrame, filename: str):
        df_columns = df.columns.tolist()

        missing_required = [c for c in REQUIRED_COLUMNS if c not in df_columns]
        phone_present = "PHONE" in df_columns or "HASHED_PHONE" in df_columns

        if missing_required or not phone_present:
            error_info = {
                "filename": filename,
                "missing_columns": missing_required
                + ([] if phone_present else ["PHONE or HASHED_PHONE"]),
                "found_columns": df_columns,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            self.schema_errors.append(error_info)

            msg = (
                f"❌ Schema validation failed for {filename}. "
                f"Missing: {error_info['missing_columns']}"
            )
            logger_ptk.error(msg)
            return False, msg

        logger_ptk.info(f"✅ Schema validation passed for {filename}")
        return True, None
    


    def validate_and_read(
        self, filename: str, content: bytes, partner_name: str = None, batch_no: int = None
    ) -> Tuple[bool, pd.DataFrame, str]:

        file_type = Path(filename).suffix.lower()

        if not content:
            return False, None, f"{filename} is empty"

        if file_type not in (".csv", ".xls", ".xlsx"):
            return False, None, f"Unsupported file type: {file_type}"

        try:
            if file_type == ".csv":
                encodings = [
                    "utf-8",
                    "utf-8-sig",
                    "latin1",
                    "utf-16",
                    "iso-8859-1",
                    "cp1252",
                ]
                df = None
                for enc in encodings:
                    try:
                        df = pd.read_csv(BytesIO(content), encoding=enc)
                        break
                    except Exception:
                        continue
                if df is None:
                    return False, None, f"Unable to read {filename}"

            else:
                sheets = pd.read_excel(
                    BytesIO(content), sheet_name=None, engine="openpyxl"
                )
                if not sheets:
                    return False, None, "Excel has no sheets"
                df = (
                    pd.concat(sheets.values(), ignore_index=True)
                    if len(sheets) > 1
                    else next(iter(sheets.values()))
                )

        except Exception as e:
            return False, None, f"Read error: {e}"

        df.columns = df.columns.str.strip()
        df.rename(columns=COLUMN_MAPPING, inplace=True)

        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].str.strip()

        is_valid, err = self.validate_schema(df, filename)
        if not is_valid:
            return False, None, err

        
        return True, df, None

    def read_files(self, filename: str, content: bytes, batch_no: int, seen_phones: set = None, partner_id: str = None):
        """
        Reads, validates, and marks duplicates in the file content using an in-memory set
        to avoid expensive Snowflake UPDATE statements.
        """
        partner_name = self.config_ptk.location.folder_name

        # 1. Validate and Read
        is_valid, df, error_msg = self.validate_and_read(filename, content, partner_name, batch_no)
        
        if not is_valid:
            return filename, None, error_msg

        try:
            if "HASHED_PHONE" not in df.columns:
                df["HASHED_PHONE"] = df["PHONE"] if "PHONE" in df.columns else None

            original_count = len(df)
            statuses = []
            infile_duplicates = 0
            inbatch_duplicates = 0
            
            # Temporary set for in-file deduplication
            file_seen_phones = set()
            
            if seen_phones is not None:
                for phone in df["HASHED_PHONE"]:
                    if pd.isna(phone) or phone == "":
                        statuses.append("not processed")
                    elif phone in seen_phones:
                        if phone in file_seen_phones:
                            infile_duplicates += 1
                        else:
                            inbatch_duplicates += 1
                        statuses.append("duplicate")
                    else:
                        statuses.append("not processed")
                        seen_phones.add(phone)
                        file_seen_phones.add(phone)
            else:
                statuses = ["not processed"] * len(df)
            
            # Store duplicate logs for UI
            self.duplicate_logs.append({
                "filename": os.path.basename(filename),
                "original_count": original_count,
                "infile_dropped": infile_duplicates,
                "inbatch_dropped": inbatch_duplicates,
                "duplicate_columns": ["HASHED_PHONE"],
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })

            # Enrich with metadata 
            df["STATUS"] = statuses
            df["PARTNER_NAME"] = partner_name
            df["BATCH_NO"] = f"batch_{batch_no}"
            df["FILENAME"] = os.path.basename(filename)
            df["PARTNER_ID"] = partner_id
            df["CREATED_AT"] = datetime.today().date()

           
            df.columns = [c.upper() for c in df.columns]

            required_cols = [c.upper() for c in file_schema.names]
            for col_name in required_cols:
                if col_name not in df.columns:
                    df[col_name] = pd.NA
            
            internal_cols = ["PARTNER_NAME", "BATCH_NO", "FILENAME", "CREATED_AT", "STATUS", "PARTNER_ID"]
            final_df = df[list(set(required_cols + internal_cols))]

            snow_df = session_ptk.create_dataframe(final_df)
            logger_ptk.info(f"Snowpark DF created | count={snow_df.count()} | file={filename}")
            
            save_raw_table(snow_df, "PARTNERS_RAW_DATA", partner_name, batch_no, os.path.basename(filename), "append")
            logger_ptk.info(f"✅ Data for {filename} saved to Snowflake")

            return filename, snow_df, None
            
        except Exception as e:
            msg = f"Failed during processing or Snowflake insertion: {e}"
            logger_ptk.error(msg)
            return filename, None, msg
                
        



    def get_schema_errors(self):
        return self.schema_errors

    def get_duplicate_logs(self):
        return self.duplicate_logs