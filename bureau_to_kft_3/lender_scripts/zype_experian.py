# SQL Queries for Lender 1
from snowflake.snowpark import Session, DataFrame
import snowflake.snowpark.functions as F
from snowflake.snowpark.functions import col, lit, concat

def get_file_ar(session: Session, df) -> DataFrame:
    
    df = df.withColumn("CUSTOMER_ID", concat(lit("ZPEX"), df["CUSTOMER_ID"]))
    return df.select(
        "CUSTOMER_ID", "ACCT_KEY", col("created_at").alias("BEREAU_DATE"), col("M_SUB_ID").alias("SECTOR"), "ACCT_TYPE_CD", "OPEN_DT", 
        col("BALANCE_AM").alias("CUR_BALANCE_AM"), col("BALANCE_DT").alias("REPORTED_DT"), "CLOSED_DT", "CREDIT_LIMIT_AM", 
        "DAYS_PAST_DUE", "LAST_PAYMENT_DT", "ORIG_LOAN_AM", "PAST_DUE_AM", "SUIT_FILED_WILLFUL_DFLT", 
        "WRITTEN_OFF_AND_SETTLED_STATUS", "RESPONSIBILITY_CD", "EMI_AMT", "INTEREST_RATE", "TENURE", 
        "DAYS_PAST_DUE_01", "DAYS_PAST_DUE_02", "DAYS_PAST_DUE_03", "DAYS_PAST_DUE_04", "DAYS_PAST_DUE_05", 
        "DAYS_PAST_DUE_06", "DAYS_PAST_DUE_07", "DAYS_PAST_DUE_08", "DAYS_PAST_DUE_09", "DAYS_PAST_DUE_10", "DAYS_PAST_DUE_11", "DAYS_PAST_DUE_12", 
        "DAYS_PAST_DUE_13", "DAYS_PAST_DUE_14", "DAYS_PAST_DUE_15", "DAYS_PAST_DUE_16", "DAYS_PAST_DUE_17", "DAYS_PAST_DUE_18", "DAYS_PAST_DUE_19", 
        "DAYS_PAST_DUE_20", "DAYS_PAST_DUE_21", "DAYS_PAST_DUE_22", "DAYS_PAST_DUE_23", "DAYS_PAST_DUE_24", "DAYS_PAST_DUE_25", "DAYS_PAST_DUE_26", 
        "DAYS_PAST_DUE_27", "DAYS_PAST_DUE_28", "DAYS_PAST_DUE_29", "DAYS_PAST_DUE_30", "DAYS_PAST_DUE_31", "DAYS_PAST_DUE_32", "DAYS_PAST_DUE_33", 
        "DAYS_PAST_DUE_34", "DAYS_PAST_DUE_35", "DAYS_PAST_DUE_36"
    )

def get_file_address(session: Session, df) -> DataFrame:
    df = df.withColumn("customer_id", concat(lit("ZPEX"), df["customer_id"]))
    return df.select(
        "customer_id", "pincode", "date_reported"
    )

def get_file_enq(session: Session, df) -> DataFrame:
    df = df.withColumn("customer_id", concat(lit("ZPEX"), df["customer_id"]))
    return df.select(
        "customer_id", col("INQ_PURP_CD").alias("PURPOSE"), "INQ_DATE", col("M_SUB_ID").alias("SECTOR"), "amount"
    )

def get_file_score(session: Session, df) -> DataFrame:
    df = df.withColumn("customer_id", concat(lit("ZPEX"), df["customer_id"]))
    return df.select(
        "customer_id", col("score_v3").alias("score")
    )

def mappings():
    zype_experian_mappings = {
    "AR": get_file_ar,
    "ADDRESS": get_file_address,
    "ENQ": get_file_enq,
    "SCORE": get_file_score
}
    return zype_experian_mappings