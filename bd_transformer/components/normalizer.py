from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import DoubleType, BooleanType, StringType

import bd_transformer.consts as const


class Normalizer:
    def __init__(self, clip: bool = False, reject: bool = False):
        """
        Spark-based normalizer for MinMax scaling.
        
        Parameters
        ----------
        clip : bool = False
            Whether to clip values during transformation and inverse transformation.
        reject : bool = False
            Whether to mark inputs out of [0, 1] as invalid when inverse transformation.
        """
        self._clip = clip
        self._reject = reject
        self._min = None
        self._max = None
        self._scale = None

    def fit(self, df: DataFrame, column_name: str) -> "Normalizer":
        """
        Fit the normalizer to a Spark DataFrame column.
        """
        # Get min and max values from the column in one pass
        from pyspark.sql.functions import min as spark_min, max as spark_max
        stats = df.select(col(column_name)).agg(
            spark_min(column_name).alias("min_val"),
            spark_max(column_name).alias("max_val")
        ).collect()[0]
        self._min, self._max = stats["min_val"], stats["max_val"]
        
        # Calculate scale, handle case where min == max
        self._scale = self._max - self._min
        self._scale = 1.0 if self._scale == 0 else self._scale
        
        return self

    def normalize(self, df: DataFrame, column_name: str) -> DataFrame:
        """
        Normalize a Spark DataFrame column using MinMax scaling.
        """
        data_col = col(column_name)
        
        # Apply clipping if enabled
        if self._clip:
            data_col = when(data_col < self._min, self._min) \
                      .when(data_col > self._max, self._max) \
                      .otherwise(data_col)
        
        # Apply MinMax scaling: (data - min) / scale
        normalized_col = (data_col - self._min) / self._scale
        
        return df.withColumn(column_name, normalized_col)

    def inverse_normalize(self, df: DataFrame, column_name: str) -> DataFrame:
        """
        Inverse normalize a Spark DataFrame column back to original scale.
        Returns DataFrame with data, valid, and error columns, preserving any row_id.
        """
        data_col = col(column_name)

        # Always start with valid=True, error=""
        valid_col = lit(True).cast(BooleanType())
        error_col = lit("").cast(StringType())

        if self._clip:
            # Clip data to [0,1] range
            data_col = when(data_col < 0, 0) \
                        .when(data_col > 1, 1) \
                        .otherwise(data_col)
        if self._reject:
            # Mark out-of-range [0,1] values as invalid
            oor_condition = (data_col < 0) | (data_col > 1)
            valid_col = ~oor_condition
            error_col = when(oor_condition, "out of range [0,1]").otherwise("")
            data_col = when(oor_condition, lit(None).cast(DoubleType())).otherwise(data_col)

        # Apply inverse scaling
        data_col = (data_col * self._scale) + self._min

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