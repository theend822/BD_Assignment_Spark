#!/usr/bin/env python3
"""
Test Spark version of BD Transformer and save results
"""

import yaml
import sys
sys.path.insert(0, "/Users/jxy/Downloads/bd_transformer_spark")

from pyspark.sql import SparkSession
from bd_transformer.transformer import Transformer


def main():
    print("=== Testing Spark Version ===")
    
    # Initialize Spark
    spark = SparkSession.builder.appName("BD_Transformer_Test").getOrCreate()
    
    try:
        # Paths
        config_path = "../../config/config.yaml"
        data_path = "../../data/001.parquet"
        
        # Load data and config
        data = spark.read.parquet(data_path)
        print(f"Loaded data: {data.count()} rows, {len(data.columns)} columns")
        
        with open(config_path, "r") as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
        
        # Run full transformation pipeline
        transformer = Transformer(config)
        transformer.fit(data)
        transformed = transformer.transform(data)
        inversed = transformer.inverse_transform(transformed)
        
        # Convert to Pandas and save results to test results directory
        data.toPandas().to_parquet("../data/spark_original.parquet")
        transformed.toPandas().to_parquet("../data/spark_transformed.parquet")
        inversed.toPandas().to_parquet("../data/spark_inversed.parquet")
        
        print("✅ Spark version completed")
        print("Results saved to:")
        print("  - spark_original.parquet") 
        print("  - spark_transformed.parquet")
        print("  - spark_inversed.parquet")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()