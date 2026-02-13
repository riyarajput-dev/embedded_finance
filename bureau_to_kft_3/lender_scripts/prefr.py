import json
import os
import snowflake.snowpark.functions as F
from snowflake.snowpark.functions import col, lit, concat
from ..src.utils.column_mapping import apply_dynamic_mapping

# Load configuration using relative path
current_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(current_dir, '..', 'config', 'prefr_experian.json')

with open(config_path, 'r') as f:
    PREFR_CONFIG = json.load(f)

def get_file_ar(session, df):
    # Apply PRFR prefix to CUSTOMER_ID if it exists in the dataframe
    if "CUSTOMER_ID" in df.columns:
        df = df.withColumn("CUSTOMER_ID", concat(lit("PRFR"), col("CUSTOMER_ID")))
    elif "customer_id" in df.columns:
        df = df.withColumn("customer_id", concat(lit("PRFR"), col("customer_id")))
        
    return apply_dynamic_mapping(df, PREFR_CONFIG['files']['AR'])

def get_file_address(session, df):
    if "CUSTOMER_ID" in df.columns:
        df = df.withColumn("CUSTOMER_ID", concat(lit("PRFR"), col("CUSTOMER_ID")))
    elif "customer_id" in df.columns:
        df = df.withColumn("customer_id", concat(lit("PRFR"), col("customer_id")))

    return apply_dynamic_mapping(df, PREFR_CONFIG['files']['ADDRESS'])

def get_file_enq(session, df):
    if "CUSTOMER_ID" in df.columns:
        df = df.withColumn("CUSTOMER_ID", concat(lit("PRFR"), col("CUSTOMER_ID")))
    elif "customer_id" in df.columns:
        df = df.withColumn("customer_id", concat(lit("PRFR"), col("customer_id")))

    return apply_dynamic_mapping(df, PREFR_CONFIG['files']['ENQ'])

def get_file_score(session, df):
    if "CUSTOMER_ID" in df.columns:
        df = df.withColumn("CUSTOMER_ID", concat(lit("PRFR"), col("CUSTOMER_ID")))
    elif "customer_id" in df.columns:
        df = df.withColumn("customer_id", concat(lit("PRFR"), col("customer_id")))

    return apply_dynamic_mapping(df, PREFR_CONFIG['files']['SCORE'])

def get_file_name_dob(session, df):
    if "CUSTOMER_ID" in df.columns:
        df = df.withColumn("CUSTOMER_ID", concat(lit("PRFR"), col("CUSTOMER_ID")))
    elif "customer_id" in df.columns:
        df = df.withColumn("customer_id", concat(lit("PRFR"), col("customer_id")))

    return apply_dynamic_mapping(df, PREFR_CONFIG['files']['NAME_DOB'])
    
def get_file_phone(session, df):
    if "CUSTOMER_ID" in df.columns:
        df = df.withColumn("CUSTOMER_ID", concat(lit("PRFR"), col("CUSTOMER_ID")))
    elif "customer_id" in df.columns:
        df = df.withColumn("customer_id", concat(lit("PRFR"), col("customer_id")))

    return apply_dynamic_mapping(df, PREFR_CONFIG['files']['PHONE'])

def get_file_id(session, df):
    if "CUSTOMER_ID" in df.columns:
        df = df.withColumn("CUSTOMER_ID", concat(lit("PRFR"), col("CUSTOMER_ID")))
    elif "customer_id" in df.columns:
        df = df.withColumn("customer_id", concat(lit("PRFR"), col("customer_id")))

    return apply_dynamic_mapping(df, PREFR_CONFIG['files']['ID'])

def get_file_email(session, df):
    if "CUSTOMER_ID" in df.columns:
        df = df.withColumn("CUSTOMER_ID", concat(lit("PRFR"), col("CUSTOMER_ID")))
    elif "customer_id" in df.columns:
        df = df.withColumn("customer_id", concat(lit("PRFR"), col("customer_id")))

    return apply_dynamic_mapping(df, PREFR_CONFIG['files']['EMAIL'])

def prefr_mappings():
    prefr_experian_mappings={
        "AR": get_file_ar,
        "ADDRESS": get_file_address,
        "ENQ": get_file_enq,
        "SCORE": get_file_score,
        "NAME_DOB": get_file_name_dob,
        "PHONE": get_file_phone,
        "ID": get_file_id,
        "EMAIL": get_file_email
    }
    return prefr_experian_mappings
