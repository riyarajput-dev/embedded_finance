from snowflake.snowpark import DataFrame
from snowflake.snowpark.functions import expr, col, trim
import re
import snowflake.snowpark.functions as F


date_cols = [
    "DATE_REPORTED",
    "INQUIRY_DATE",
    "REPORTED_DATE",
    "DISBURSED_DT",
    "CLOSE_DT",
    "CLOSED_DT",
    "LAST_PAYMENT_DATE",
    "WRITE_OFF_DATE",
    "DOB",
    "DATE_OF_BIRTH",
    "RETRO_DT",
    "INQ_DATE",
    "BALANCE_DT",
    "OPEN_DT",
    'DFLT_STATUS_DT',
    'LAST_PAYMENT_DT','DATECLOSED','DATEOPENED','DATEREPORTED','OFFER_EXPIRY_DATE','CIBIL_SCRUB_DATE'
]

amount_cols = [
    "CREDIT_LIMIT",
    "DISBURSED_AMT",
    "CURRENT_BAL",
    "OVERDUE_AMT",
    "WRITE_OFF_AMT",
    "INCOME",
    "AMOUNT",
    "MONTHLY_INCOME", "APPROVED_LOAN_AMOUNT", "MIN_TOP_UP_AMOUNT"
]

class Parse:
    def parse_date_on_df(self, df: DataFrame, colm: str) -> DataFrame:
        """Parse date column on a specific DataFrame with better handling of edge cases"""
        
        df = df.withColumn(colm, trim(col(colm)))

        df = df.withColumn(colm, col(colm).cast("STRING"))
        
        date_sql = f"""
            COALESCE(
                TRY_TO_DATE({colm}, 'DD/MM/YYYY'),
                TRY_TO_DATE({colm}, 'DD-MM-YYYY'),
                TRY_TO_DATE({colm}, 'DD.MM.YYYY'),
                TRY_TO_DATE({colm}, 'YYYY/MM/DD'),
                TRY_TO_DATE({colm}, 'YYYY-MM-DD'),
                TRY_TO_DATE({colm}, 'YYYY.MM.DD'),
                TRY_TO_DATE({colm}, 'MM/DD/YYYY'),
                TRY_TO_DATE({colm}, 'MM-DD-YYYY'),
                TRY_TO_DATE({colm}, 'DD/MM/YY'),
                TRY_TO_DATE({colm}, 'DD-MM-YY'),
                TRY_TO_DATE({colm}, 'DD/MON/YYYY'),
                TRY_TO_DATE({colm}, 'DD-MON-YYYY'),
                TRY_TO_DATE({colm}, 'DD/MMM/YYYY'),
                TRY_TO_DATE({colm}, 'DD-MMM-YYYY'),
                TRY_TO_DATE({colm})
            )
        """
        
        return df.withColumn(colm, expr(date_sql))


    def parse_amounts_on_df(self, df: DataFrame, colm: str) -> DataFrame:
        """Parse amount column on a specific DataFrame"""
        from snowflake.snowpark.functions import expr

        amount_sql = f"""
            TRY_TO_NUMBER(
                REGEXP_REPLACE({colm}, '[^0-9.-]', ''),
                10, 2
            )
        """
        return df.withColumn(colm, expr(amount_sql))

    def parse_file_df(self, df: DataFrame) -> DataFrame:
        """Parse a DataFrame (operates on parameter, not self.data)"""
        dates = set(date_cols).intersection(df.columns)
        final_cols = set(amount_cols).intersection(df.columns)
        number_cols = sorted(list(final_cols))

        if "PHONE" in df.columns:
            df = df.withColumn(
                "PHONE",
                F.when(
                    F.col("PHONE").isNotNull(), F.substr(F.col("PHONE"), -10, 10)
                ).otherwise(None),
            )

        for col in dates:
            df = self.parse_date_on_df(df, col)

        for col in number_cols:
            df = self.parse_amounts_on_df(df, col)

        return df
    