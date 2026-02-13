from snowflake.snowpark import DataFrame
import snowflake.snowpark.functions as F
from snowflake.snowpark.functions import col, lit
import logging
from ...src import aws_credentials, logger_btk
logger = logger_btk


def apply_dynamic_mapping(df: DataFrame, table_config: dict) -> DataFrame:
    """
    Applies column selection and transformation based on the provided configuration.
    
    Args:
        df: Input Snowpark DataFrame.
        table_config: Dictionary containing 'columns' list from the JSON config.
                      Example: {'columns': [{'source': 'col1', 'target': 'COL1'}, ...]}
    
    Returns:
        DataFrame with selected and transformed columns.
    """
    if not table_config or 'columns' not in table_config:
        logger.warning("No column configuration found. Returning original DataFrame.")
        return df

    selected_cols = []
    
    for column_def in table_config['columns']:
        target_col = column_def.get('target')
        source_col = column_def.get('source')
        default_val = column_def.get('default')
        transform_expr = column_def.get('transform')
        
        if not target_col:
            continue
            
        current_expr = None
        
        # Priority 1: Transformation Expression
        if transform_expr:
            # If there's a transform expression, use it directly
            # Note: transform expressions in JSON might use SQL syntax or Snowpark functions
            # Assuming SQL expressions for now as seen in config (e.g. TO_CHAR(dob))
             current_expr = F.expr(transform_expr)
             
        # Priority 2: Source Column
        elif source_col:
            # If source column exists in DF, use it
            # We might need to handle case sensitivity or missing columns
            current_expr = col(source_col)
            
        # Priority 3: Default Value
        elif default_val is not None:
            current_expr = lit(default_val)
            
        else:
             # If mapping is defined but no source/default/transform, 
             # and it's not in the DF, we might want to create it as null or skip
             # For now, let's treat it as a literal NULL if nothing else matches
             current_expr = lit(None)

        if current_expr is not None:
            selected_cols.append(current_expr.alias(target_col))
            
    if not selected_cols:
        logger.warning("No valid columns generated from config. Returning original DataFrame.")
        return df
        
    return df.select(selected_cols)
