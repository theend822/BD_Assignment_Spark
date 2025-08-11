#!/usr/bin/env python3
"""
Test pandas version of BD Transformer and save results
"""

import pandas as pd
import yaml
import sys
sys.path.insert(0, "/Users/jxy/Downloads/bd_transformer_original")

from bd_transformer.transformer import Transformer


def main():
    print("=== Testing Pandas Version ===")
    
    # Paths
    config_path = "/Users/jxy/Downloads/bd_transformer_original/config.yaml"
    data_path = "/Users/jxy/Downloads/bd_transformer_original/data/small/001.parquet"
    
    # Load data and config
    data = pd.read_parquet(data_path)
    print(f"Loaded data: {data.shape}")
    
    with open(config_path, "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    
    # Run transformation
    transformer = Transformer(config)
    transformer.fit(data)
    transformed = transformer.transform(data.copy())
    inversed = transformer.inverse_transform(transformed.copy())
    
    # Save results to test results directory
    data.to_parquet("../results/pandas_original.parquet")
    transformed.to_parquet("../results/pandas_transformed.parquet")
    inversed.to_parquet("../results/pandas_inversed.parquet")
    
    print("✅ Pandas version completed")
    print("Results saved to:")
    print("  - pandas_original.parquet")
    print("  - pandas_transformed.parquet")
    print("  - pandas_inversed.parquet") 


if __name__ == "__main__":
    main()