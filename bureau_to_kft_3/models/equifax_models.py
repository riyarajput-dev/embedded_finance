from snowflake.snowpark.types import StringType, StructField, StructType


schema_for_ar_file = StructType(
    [
        StructField("REPORTNUMBER", StringType()),
        StructField("REFERENCE_NO", StringType(),nullable=False),
        StructField("ACCT_NUMBER", StringType()),
        StructField("ACCOUNTSTATUS", StringType()),
        StructField("ACCOUNTTYPE", StringType()),
        StructField("ASSETCLASSIFICATION", StringType()),
        StructField("BALANCE", StringType()),
        StructField("SANCTIONAMOUNT", StringType()),
        StructField("COLLATERALTYPE", StringType()),
        StructField("COLLATERALVALUE", StringType()),
        StructField("CREDITLIMIT", StringType()),
        StructField("DATECLOSED", StringType()),
        StructField("DATEOPENED", StringType()),
        StructField("DATEREPORTED", StringType()),
        StructField("HIGHCREDIT", StringType()),
        StructField("INSTALLMENTAMOUNT", StringType()),
        StructField("INTERESTRATE", StringType()),
        StructField("LASTPAYMENT", StringType()),
        StructField("LASTPAYMENTDATE", StringType()),
        StructField("OWNERSHIPTYPE", StringType()),
        StructField("PASTDUEAMOUNT", StringType()), 
        StructField("REPAYMENTTENURE", StringType()),
        StructField("TERMFREQUENCY", StringType()),
        StructField("TERMS_FREQUENCY", StringType()),
        StructField("SUITFILEDSTATUS", StringType()),
        StructField("WRITEOFFAMOUNT", StringType()),
        StructField("ACCOUNTSTATUS_HISTORY", StringType()),
        StructField("ASSETCLASS_HISTORY", StringType()),
        StructField("SUITFILED_HISTORY", StringType()),
        StructField("ACCT_UNIQ_ID", StringType()),
        StructField("CSMR_NBR", StringType()),
        StructField("CLIENT_ID", StringType()),
        StructField("SCORE", StringType()),
        StructField("SELF_TRADE", StringType()),
        StructField("SECTOR", StringType()),
        StructField("DAYS_PAST_DUE", StringType()),
    ]
)

schema_for_enq_file = StructType(
    [
        StructField("REFERENCE_NO", StringType(),nullable=False),
        StructField("TRAN_ID", StringType()),
        StructField("INQ_TIME", StringType()),
        StructField("TRAN_AMT", StringType()),
        StructField("INQ_PURPOSE", StringType()),
        StructField("SELF_TRADE", StringType()),
        StructField("SECTOR", StringType())

    ])

schema_for_pii_file = StructType(
    [
        StructField("REFERENCE_NO", StringType(),nullable=False),
        StructField("Matched_ACCT_NBR", StringType()),
        StructField("EMAIL_ADDR_1", StringType()),
        StructField("EMAIL_ADDR_1_DT_RPTED", StringType()),
        StructField("BRTH_DT", StringType()),
        StructField("PHN_NBR", StringType()),
        StructField("PHN_NBR_2", StringType()),
        StructField("PHN_NBR_3", StringType()),
        StructField("ADDR_LN", StringType()),
        StructField("ADDR_LN_2", StringType()),
        StructField("ADDR_LN_3", StringType()),
        StructField("ADDR_ST", StringType()),
        StructField("ADDR_ST_2", StringType()),
        StructField("ADDR_ST_3", StringType()),
        StructField("ADDR_1_CTY", StringType()),
        StructField("ADDR_1_ZIP", StringType()),
        StructField("ADDR_2_ZIP", StringType()),
        StructField("ADDR_3_ZIP", StringType()),
        StructField("ADDR_1_CHNG_DT", StringType()),
        StructField("ADDR_2_CHNG_DT", StringType()),
        StructField("ADDR_3_CHNG_DT", StringType()),
        StructField("acct_uniq_id", StringType()),
        StructField("rec_seq_nbr", StringType()),
        StructField("Matched_csmr_NBR", StringType()),
        StructField("PAN", StringType()),
        StructField("PASSPORT", StringType()),
        StructField("VOTER", StringType()),
        StructField("DL", StringType()),
        StructField("Adhar", StringType()),
        StructField("IN_NAME", StringType()),
        StructField("ROUTE_FLG", StringType()),
        StructField("AGE", StringType()),
        StructField("GENDER_CD", StringType()),

    ])