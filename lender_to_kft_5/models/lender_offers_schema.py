from snowflake.snowpark.types import StringType, StructField, StructType
schema = StructType(
    [
        StructField("CUSTOMER_NUMBER", StringType(),nullable=False),
        StructField("MOBILE_NO", StringType()),
        StructField("CUSTOMER_NAME", StringType()),
        StructField("PERSONAL_EMAIL_ID", StringType()),
        StructField("GENDER", StringType()),
        StructField("RESI_PINCODE", StringType()),
        StructField("PAN_NO", StringType()),
        StructField("MONTHLY_INCOME", StringType()),
        StructField("OFFER_EXPIRY_DATE", StringType()),
        StructField("APPROVED_LOAN_AMOUNT", StringType()),
        StructField("ROI", StringType()),
        StructField("CIBIL_SCRUB_DATE", StringType()),
        StructField("MIN_TENURE_BY_POLICY", StringType()),
        StructField("MAX_TENURE_BY_POLICY", StringType()),
        StructField("MAX_EMI", StringType()),
        StructField("MIN_TOP_UP_AMOUNT", StringType()),
        StructField("PROCESSING_FESS", StringType()),
        StructField("OFFER_STATUS", StringType()),
        StructField("PERSONAL_EMAIL_ID2", StringType()),
        StructField("OVERLAP_FLAG", StringType())
    ]
)
