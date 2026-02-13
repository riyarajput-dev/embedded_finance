import json
import os
import snowflake.snowpark.functions as F
from snowflake.snowpark.functions import col, lit, concat
from ..src.utils.column_mapping import apply_dynamic_mapping

# Load configuration using relative path
current_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(current_dir, '..', 'config', 'zype_equifax.json')

with open(config_path, 'r') as f:
    ZYPE_CONFIG = json.load(f)

def get_file_tl(session, df):
    # AR file in config
    if "REFERENCE_NO" in df.columns:
        df = df.withColumn("REFERENCE_NO", concat(lit("ZPEQ"), col("REFERENCE_NO")))
    elif "reference_no" in df.columns:
         df = df.withColumn("reference_no", concat(lit("ZPEQ"), col("reference_no")))

    return apply_dynamic_mapping(df, ZYPE_CONFIG['files']['AR'])

def get_file_address(session, df):
    # PII file in code maps to ADDRESS in config
    if "REFERENCE_NO" in df.columns:
        df = df.withColumn("REFERENCE_NO", concat(lit("ZPEQ"), col("REFERENCE_NO")))
    elif "reference_no" in df.columns:
         df = df.withColumn("reference_no", concat(lit("ZPEQ"), col("reference_no")))

    return apply_dynamic_mapping(df, ZYPE_CONFIG['files']['ADDRESS'])

def get_file_enq(session, df):
    # ENQ file in config
    if "REFERENCE_NO" in df.columns:
        df = df.withColumn("REFERENCE_NO", concat(lit("ZPEQ"), col("REFERENCE_NO")))
    elif "reference_no" in df.columns:
         df = df.withColumn("reference_no", concat(lit("ZPEQ"), col("reference_no")))

    return apply_dynamic_mapping(df, ZYPE_CONFIG['files']['ENQ'])

def zype_eq_mappings():
    zype_equifax_mappings={
        "AR": get_file_tl,
        "PII": get_file_address,
        "ENQ": get_file_enq
    }
    return zype_equifax_mappings
