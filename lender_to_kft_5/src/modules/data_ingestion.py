

from ..modules.file_parser import Parse
from ..modules.file_reader import ReadFile
from ...src import config_ltk, logger_ltk
from ...models.lender_offers_schema import schema as lender_schema
from ...models.required_schema import required_schema
from datetime import datetime
import json
import snowflake.snowpark.functions as F
from ..modules import save_table


class IngestData:
    def __init__(self, config_ltk):
        self.config = config_ltk

    
    
    def map_to_required_schema(self, df):
        """Map the current lender schema to the required standard schema"""
        logger_ltk.info("Mapping lender data to required schema...")
        
        mapping = {
            "LOAN_AMOUNT": "APPROVED_LOAN_AMOUNT",
            "ROI": "ROI",
            "TENURE": "MAX_TENURE_BY_POLICY",
            "OFFER_EXPIRY_DATE": "OFFER_EXPIRY_DATE",
            "LENDER_UID":"CUSTOMER_NUMBER"
        }
        
        mapped_source_cols = list(mapping.values())
        
        all_cols = df.columns
        attribute_cols = [c for c in all_cols if c not in mapped_source_cols]
        
        # Create LENDERS_ATTRIBUTE as JSON
        # Note: Snowpark object_construct can be used here
        df = df.withColumn("LENDERS_ATTRIBUTE", F.to_json(F.object_construct(*[F.lit(c) if i % 2 == 0 else F.col(c) for i, c in enumerate([val for pair in [[c, c] for c in attribute_cols] for val in pair])])))
        
        df = df.withColumn("CREATED_AT", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("LENDER_NAME", F.lit("0"))
        
        for target, source in mapping.items():
            if source in df.columns:
                df = df.withColumn(target, F.col(source))
            else:
                df = df.withColumn(target, F.lit(None))
        
        required_cols = [field.name for field in required_schema.fields]
        df = df.select(*required_cols)
        
        return df

    def process_file(self):
        df = ReadFile(self.config).read_file(lender_schema)
        if df is None:
            logger_ltk.error("No data frame returned from ReadFile")
            return
            
        parsed_df = Parse().parse_file_df(df)
        mapped_df = self.map_to_required_schema(parsed_df)
        
        
        # save_table(mapped_df, "embed_db_test.lenders_data.lenders_offers", mode="append")
        pcsv = mapped_df.to_pandas()
        output_path = r"E:\Riya Rajput\Embedded_finance\Embedded_finance\output.csv"
        pcsv.to_csv(output_path, index=False)
        logger_ltk.info(f"Successfully saved mapped data to {output_path}")
        
        
        
if __name__ == "__main__":
    res = IngestData(config_ltk).process_file()
    
    