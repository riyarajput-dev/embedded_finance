from snowflake.snowpark import DataFrame
from ..pipeline import (
    date_cols,
    amount_cols
)
import re
import snowflake.snowpark.functions as F



class Parse:
    def parse_date_on_df(self, df: DataFrame, colm: str) -> DataFrame:
        """Parse date column on a specific DataFrame"""
        from snowflake.snowpark.functions import expr

        date_sql = f"""
            COALESCE(
                TRY_TO_DATE({colm}, 'DD/MM/YYYY'),
                TRY_TO_DATE({colm}, 'DD-MM-YYYY'),
                TRY_TO_DATE({colm}, 'DD.MM.YYYY'),
                TRY_TO_DATE({colm}, 'YYYY/MM/DD'),
                TRY_TO_DATE({colm}, 'YYYY-MM-DD'),
                TRY_TO_DATE({colm}, 'YYYY.MM.DD'),
                TRY_TO_DATE({colm}, 'MM/DD/YYYY'),
                TRY_TO_DATE({colm}, 'MM-DD-YYYY')
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
        dates = set(
            date_cols + [i for i in df.columns if re.search(r"_DT", str(i))]
        ).intersection(df.columns)

        cols1 = ["DAYS_PAST_DUE", "AMOUNT", "SCORE_V3"] + amount_cols
        cols2 = [i for i in df.columns if re.search(r"AM_[0-9]+|DUE_[0-9]+", str(i))]
        final_cols = set(cols1 + cols2).intersection(df.columns)
        number_cols = sorted(list(final_cols))

        if "PHONE" in df.columns:
            df = df.withColumn(
                "PHONE",
                F.when(
                    F.col("PHONE").isNotNull(), F.substr(F.col("PHONE"), -10, 10)
                ).otherwise(None),
            )

        if "BALANCE_DT" in df.columns:
            df = df.filter(F.col("BALANCE_DT") != "BALANCE_DT")
            df = df.filter(F.col("BALANCE_DT") != "balance_dt")

        for col in dates:
            df = self.parse_date_on_df(df, col)

        for col in number_cols:
            df = self.parse_amounts_on_df(df, col)

        return df
    