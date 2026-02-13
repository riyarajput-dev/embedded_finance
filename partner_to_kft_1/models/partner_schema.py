from snowflake.snowpark.types import StringType, StructField, StructType

file_schema = StructType([
    StructField("PARTNER_REFERENCE_ID", StringType(), nullable=False),
    StructField("HASHED_PHONE", StringType()),
    StructField("MASKED_PHONE", StringType()),
    StructField("PHONE", StringType()),
    StructField("PAN_NO", StringType()),
    StructField("NAME", StringType()),
    StructField("PARTNER_NAME", StringType()),
])

