from typing import Union

import numpy as np
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col, regexp_replace, when, isnan, isnull, lit, round as spark_round, concat
from pyspark.sql.types import DoubleType, StringType, BooleanType

import bd_transformer.consts as const


def single_string_to_number(
    data: str,
    prefix: str = "",
    suffix: str = "",
) -> Union[float, int]:
    """
    Convert a string to number, handling prefix and suffix.
    """
    prefix_len = len(prefix)
    suffix_len = len(suffix)
    if suffix_len > 0:
        data = data[prefix_len:-suffix_len]
    else:
        data = data[prefix_len:]
    return float(data)


class Converter:
    def __init__(
        self,
        min_val: Union[str, int, float, bool] = False,
        max_val: Union[str, int, float, bool] = False,
        clip_oor: bool = True,
        prefix: str = "",
        suffix: str = "",
        rounding: int = 6,
    ):
        """
        Spark-based converter for transforming string/numeric data.
        
        Parameters
        ----------
        min_val : number | bool = False
            The minimum value of the column.
        max_val : number | bool = False
            The maximum value of the column.
        clip_oor : bool = True
            Whether to clip out-of-range values.
        prefix : str = ""
            The prefix of the string representation of the values.
        suffix : str = ""
            The suffix of the string representation of the values.
        rounding : int = 6
            The number of decimal places to round the values to during inverse_convert().
        """
        self._min_val = min_val
        self._max_val = max_val
        self._clip_oor = clip_oor
        self._prefix = prefix
        self._suffix = suffix
        self._rounding = rounding
        self._original_type = None

    def fit(self, df: DataFrame, column_name: str) -> "Converter":
        """
        Fit the converter to a Spark DataFrame column.
        """
        # Determine original data type
        self._original_type = dict(df.dtypes)[column_name]
        
        # Convert string data to numeric for min/max calculation if needed
        if self._original_type == "string":
            # Remove prefix and suffix from strings, then cast to double
            import re
            temp_col = col(column_name)
            if self._prefix:
                temp_col = regexp_replace(temp_col, f"^{re.escape(self._prefix)}", "")
            if self._suffix:
                temp_col = regexp_replace(temp_col, f"{re.escape(self._suffix)}$", "")
            temp_col = temp_col.cast(DoubleType())
            
            # Get min/max from processed numeric data
            from pyspark.sql.functions import min as spark_min, max as spark_max
            stats = df.select(temp_col.alias("temp")).agg(
                spark_min("temp").alias("min_val"),
                spark_max("temp").alias("max_val")
            ).collect()[0]
            data_min, data_max = stats["min_val"], stats["max_val"]
        else:
            # For numeric columns, get min/max directly
            from pyspark.sql.functions import min as spark_min, max as spark_max
            stats = df.select(col(column_name)).agg(
                spark_min(column_name).alias("min_val"),
                spark_max(column_name).alias("max_val")
            ).collect()[0]
            data_min, data_max = stats["min_val"], stats["max_val"]

        # Set min_val based on configuration
        if isinstance(self._min_val, bool) and self._min_val:
            self._min_val = data_min
        elif isinstance(self._min_val, str):
            self._min_val = single_string_to_number(
                self._min_val,
                self._prefix,
                self._suffix,
            )
        elif isinstance(self._min_val, bool) and not self._min_val:
            self._min_val = float('-inf')

        # Set max_val based on configuration
        if isinstance(self._max_val, bool) and self._max_val:
            self._max_val = data_max
        elif isinstance(self._max_val, str):
            self._max_val = single_string_to_number(
                self._max_val,
                self._prefix,
                self._suffix,
            )
        elif isinstance(self._max_val, bool) and not self._max_val:
            self._max_val = float('inf')
            
        return self

    def convert(self, df: DataFrame, column_name: str) -> DataFrame:
        """
        Convert a Spark DataFrame column to numeric values.
        """
        new_col = col(column_name)
        
        # Handle string to numeric conversion
        if self._original_type == "string":
            import re
            if self._prefix:
                new_col = regexp_replace(new_col, f"^{re.escape(self._prefix)}", "")
            if self._suffix:
                new_col = regexp_replace(new_col, f"{re.escape(self._suffix)}$", "")
            new_col = new_col.cast(DoubleType())
        
        # Apply clipping if enabled
        if self._clip_oor:
            new_col = when(new_col < self._min_val, self._min_val) \
                     .when(new_col > self._max_val, self._max_val) \
                     .otherwise(new_col)
        
        return df.withColumn(column_name, new_col)

    def inverse_convert(self, df: DataFrame, column_name: str) -> DataFrame:
        """
        Inverse convert a Spark DataFrame column back to original format.
        Returns DataFrame with data, valid, and error columns.
        """
        data_col = col(column_name)
        
        # Create valid and error columns
        if self._clip_oor:
            # If clipping, clip the data and mark all as valid
            data_col = when(data_col < self._min_val, self._min_val) \
                      .when(data_col > self._max_val, self._max_val) \
                      .otherwise(data_col)
            valid_col = lit(True).cast(BooleanType())
            error_col = lit("").cast(StringType())
        else:
            # If not clipping, mark out-of-range values as invalid
            oor_condition = (data_col < self._min_val) | (data_col > self._max_val)
            valid_col = ~oor_condition
            error_col = when(oor_condition, "Out of range").otherwise("")
            # Set out-of-range values to null
            data_col = when(oor_condition, lit(None).cast(DoubleType())).otherwise(data_col)
        
        # Round the data
        data_col = spark_round(data_col, self._rounding)
        
        # Convert back to original type if it was string
        if self._original_type == "string":
            data_col = data_col.cast(StringType())
            if self._prefix or self._suffix:
                data_col = concat(lit(self._prefix), data_col, lit(self._suffix))
        
        # Preserve row_id if it exists in the input DataFrame
        select_cols = [
            data_col.alias(const.DATA_COL_NAME),
            valid_col.alias(const.VALID_COL_NAME),
            error_col.alias(const.ERROR_COL_NAME)
        ]
        
        # Check if row_id column exists and preserve it
        if "row_id" in df.columns:
            select_cols.append(col("row_id"))

        return df.select(*select_cols)