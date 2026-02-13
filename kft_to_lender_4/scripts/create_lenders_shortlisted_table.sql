-- Snowflake Table Schema for Lenders Shortlisted Data
-- This table stores metadata about shortlisted customer records sent to lenders

CREATE OR REPLACE TABLE EMBED_DB_TEST.LENDERS_DATA.LENDERS_SHORTLISTED (
    CREATED_AT DATE,
    BUREAU_ID VARCHAR(16777216),
    LENDER_UID VARCHAR(16777216),
    LENDER_BATCH VARCHAR(16777216),
    LENDER_NAME VARCHAR(16777216),
    BUREAU_TYPE VARCHAR(16777216)
);

-- Description:
-- CREATED_AT: Date when the record was created
-- BUREAU_ID: Extracted bureau identifier (substring of LENDER_UID from position 5)
-- LENDER_UID: Unique identifier (REFERENCE_NO for Equifax, CUSTOMER_ID for Experian)
-- LENDER_BATCH: Batch name/identifier for the lender processing
-- LENDER_NAME: Name of the lender (e.g., 'prefr', 'zype')
-- BUREAU_TYPE: Type of bureau (e.g., 'equifax', 'experian')
