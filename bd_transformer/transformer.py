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
        # Add unique row IDs ONCE at the beginning for efficient joining
        from pyspark.sql.functions import monotonically_increasing_id
        
        result_df = data.withColumn("row_id", monotonically_increasing_id())
        
        for column_name in self.config:
            # Apply inverse normalization - now preserves row_id automatically
            inverse_normalized = self.normalizers[column_name].inverse_normalize(
                result_df, column_name
            )
            
            # Apply inverse conversion - also preserves row_id automatically
            inverse_converted = self.converters[column_name].inverse_convert(
                inverse_normalized, const.DATA_COL_NAME
            )
            
            # Join on row IDs and replace the target column
            result_df = result_df.join(
                inverse_converted.select("row_id", col(const.DATA_COL_NAME).alias(f"new_{column_name}")),
                on="row_id"
            ).select(
                # Select all columns, replacing the target column with converted data
                *[col(c) if c != column_name else col(f"new_{column_name}").alias(column_name) 
                  for c in result_df.columns if c != "row_id"],  # Keep other columns
                "row_id"  # Keep row_id for next iteration
            )
            
        # Remove row IDs at the end
        return result_df.drop("row_id")