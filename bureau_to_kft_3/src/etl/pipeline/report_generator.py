import pandas as pd
from ....src import snowflake_connection_obj, config_btk, logger_btk
import re

session_btk = snowflake_connection_obj(config_btk.database, config_btk.schema)
logger = logger_btk
standard_file_names = [
    "ADDRESS",
    "AR",
    "EMAIL",
    "ENQ",
    "ID",
    "NAME_DOB",
    "PHONE",
    "SCORE",
    "EMPLOYMENT",
    "FINTECH_ENQUIRIES",
    "PROPENSITY_SCORE",
]


def summary(file_name, status, batch, og_count, fin_count):
    
    summary_df = pd.DataFrame(
        columns=[
            "FILE_NAME",
            "STATUS",
            "BATCH",
            "ORIGINAL_COUNT",
            "FINAL_COUNT",
            "DUPLICATES_AND_NULLS",
        ]
    )
    summary_df["FILE_NAME"] = [file_name]
    summary_df["BATCH"] = [batch]
    summary_df["ORIGINAL_COUNT"] = [og_count]
    summary_df["FINAL_COUNT"] = [fin_count]
    summary_df["DUPLICATES_AND_NULLS"] = [og_count - fin_count]
    summary_df["STATUS"] = [status]
    summary_df["COUNT"] = [1]

    return summary_df

def pivot_df(df):
    
    for file in standard_file_names:
        index = df["FILE_NAME"].apply(lambda x: find_pattern(x, file))
        df.loc[index, "FILE_NAME"] = file

    pivot = (
        df.groupby(["FILE_NAME", "BATCH", "STATUS"])
        .agg(
            {
                "COUNT": "sum",
                "ORIGINAL_COUNT": "sum",
                "FINAL_COUNT": "sum",
                "DUPLICATES_AND_NULLS": "sum",
            }
        )
        .reset_index()
        .rename(columns={"COUNT": "NNR_OF_FILES"})
    )

    return pivot


def find_pattern(text, pattern):
    return re.search(rf".*{re.escape(pattern.lower())}.*", text.lower()) is not None


def saved_data_summary():
    return f"""

select * from (
select 'ID' as FILE_NAME, identity_tag as BATCH, count(*) AS FINAL_COUNT from embed_db_test.experian_dev.ID
where identity_tag = 'batch_1' group by 1,2
union
select 'NAME_DOB' as FILE_NAME, identity_tag as BATCH, count(*) AS FINAL_COUNT from embed_db_test.experian_dev.NAME_DOB
where identity_tag = 'batch_1' group by 1,2
union
select 'PHONE' as FILE_NAME, identity_tag as BATCH, count(*) AS FINAL_COUNT from embed_db_test.experian_dev.PHONE
where identity_tag = 'batch_1' group by 1,2
union
select 'ADDRESS' as FILE_NAME, identity_tag as BATCH, count(*) AS FINAL_COUNT from embed_db_test.experian_dev.ADDRESS
where identity_tag = 'batch_1' group by 1,2
union
select 'EMPLOYMENT' as FILE_NAME, identity_tag as BATCH, count(*) AS FINAL_COUNT from embed_db_test.experian_dev.EMPLOYMENT
where identity_tag = 'batch_1' group by 1,2
union
select 'SCORE' as FILE_NAME, identity_tag as BATCH, count(*) AS FINAL_COUNT from embed_db_test.experian_dev.SCORE
where identity_tag = 'batch_1' group by 1,2 
union
select 'ENQ' as FILE_NAME, identity_tag as BATCH, count(*) AS FINAL_COUNT from embed_db_test.experian_dev.ENQ
where identity_tag = 'batch_1' group by 1,2
union
select 'EMAIL' as FILE_NAME, identity_tag as BATCH, count(*) AS FINAL_COUNT from embed_db_test.experian_dev.EMAIL
where identity_tag = 'batch_1' group by 1,2
union
select 'AR' as FILE_NAME, identity_tag as BATCH, count(*) AS FINAL_COUNT from embed_db_test.experian_dev.AR
where identity_tag = 'batch_1' group by 1,2

) as result

"""


def save():
    identity_tag = f"{config_btk.location.identity_tag}".lower()
    demog = session_btk.sql(saved_data_summary())

    df = demog.toPandas()

    return df
