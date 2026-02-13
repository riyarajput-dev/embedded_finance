from snowflake.snowpark.types import StringType, StructField, StructType
required_schema = StructType(
    [
        StructField("CREATED_AT", StringType(),nullable=False),
        StructField("LENDER_UID", StringType()),
        StructField("LENDER_NAME", StringType()),
        StructField("LOAN_AMOUNT", StringType()),
        StructField("ROI", StringType()),
        StructField("TENURE", StringType()),
        StructField("OFFER_EXPIRY_DATE", StringType()),
        StructField("LENDERS_ATTRIBUTE", StringType()),
       
    ])