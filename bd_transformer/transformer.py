from pyspark.sql import DataFrame
from pyspark.sql.functions import col

import bd_transformer.consts as const
from bd_transformer.components.converter import Converter
from bd_transformer.components.normalizer import Normalizer


class Transformer:
    """
    A Spark-based transformer that converts and normalizes data according to a given configuration.
    """

    def __init__(self, config: dict):
        self.config = config
        self.converters = {}
        self.normalizers = {}
        
    def fit(self, data: DataFrame) -> "Transformer":
        """
        Fit the transformer parameters to the Spark DataFrame.
        """
        for column_name in self.config:
            # Initialize and fit converter
            self.converters[column_name] = Converter(
                **self.config[column_name].get("converter", {})
            ).fit(data, column_name)
            
            # Convert data and fit normalizer
            converted_data = self.converters[column_name].convert(data, column_name)
            self.normalizers[column_name] = Normalizer(
                **self.config[column_name].get("normalizer", {})
            ).fit(converted_data, column_name)
            
        return self

    def transform(self, data: DataFrame) -> DataFrame:
        """
        Transform the Spark DataFrame according to the configuration and fitted parameters.
        """
        result_df = data
        
        for column_name in self.config:
            # Apply conversion
            result_df = self.converters[column_name].convert(result_df, column_name)
            # Apply normalization
            result_df = self.normalizers[column_name].normalize(result_df, column_name)
            
        return result_df

    def inverse_transform(self, data: DataFrame) -> DataFrame:
        """
        Inverse transform using pandas-style approach with row-aligned operations.
        """
        col_list = []
        
        for column_name in self.config:
            # Step 1: Get tuple from normalizer
            data_expr, valid_expr, error_expr = self.normalizers[column_name].inverse_normalize(column_name)
            
            # Step 2: Pass tuple to converter, get modified tuple back
            data_expr, valid_expr, error_expr = self.converters[column_name].inverse_convert(
                data_expr, valid_expr, error_expr
            )
            
            # Step 3: Add expressions to list with proper aliases
            col_list.extend([
                data_expr.alias(f"{column_name}_data"),
                valid_expr.alias(f"{column_name}_valid"),
                error_expr.alias(f"{column_name}_error")
            ])
        
        # Step 4: Create final DataFrame here (and only here!)
        return data.select(*col_list)