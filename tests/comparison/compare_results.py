#!/usr/bin/env python3
"""
Compare saved results from pandas and Spark versions
"""

import pandas as pd


def compare_dataframes(df1, df2, name1="Pandas", name2="Spark"):
    """Compare two dataframes and return percentage differences"""
    print(f"\n=== Comparing {name1} vs {name2} ===")
    
    # Check shapes
    if df1.shape != df2.shape:
        print(f"Shape mismatch: {name1} {df1.shape} vs {name2} {df2.shape}")
        return
    
    print(f"Shape: {df1.shape}")
    
    # Compare each column
    total_differences = 0
    total_values = 0
    
    for col in df1.columns:
        col1, col2 = df1[col], df2[col]
        
        # Count differences with tolerance for floating point
        if col1.dtype in ['object', 'string'] or col2.dtype in ['object', 'string']:
            # String comparison
            differences = (col1.astype(str) != col2.astype(str)).sum()
        else:
            # Numeric comparison with small tolerance
            differences = (~pd.isna(col1) & ~pd.isna(col2) & (abs(col1 - col2) > 1e-10)).sum()
            # Add NaN mismatches
            differences += (pd.isna(col1) != pd.isna(col2)).sum()
        
        total_differences += differences
        total_values += len(col1)
        
        diff_percentage = (differences / len(col1)) * 100
        
        if differences > 0:
            print(f"Column '{col}': {differences}/{len(col1)} different ({diff_percentage:.4f}%)")
            
            # Show a few examples of differences
            if col1.dtype in ['object', 'string'] or col2.dtype in ['object', 'string']:
                diff_mask = col1.astype(str) != col2.astype(str)
            else:
                diff_mask = (~pd.isna(col1) & ~pd.isna(col2) & (abs(col1 - col2) > 1e-10)) | (pd.isna(col1) != pd.isna(col2))
            
            if diff_mask.any():
                examples = df1[diff_mask].head(3)
                print(f"  Examples (first 3):")
                for idx in examples.index[:3]:
                    print(f"    Row {idx}: {name1}={col1.iloc[idx]} | {name2}={col2.iloc[idx]}")
        else:
            print(f"Column '{col}': Identical")
    
    # Overall summary
    overall_percentage = (total_differences / total_values) * 100
    print(f"\n Overall: {total_differences}/{total_values} different ({overall_percentage:.4f}%)")
    
    if total_differences == 0:
        print("DataFrames are IDENTICAL!")
    else:
        print(f"DataFrames have {overall_percentage:.4f}% differences")
    
    return overall_percentage


def main():
    print("🔍 BD Transformer Results Comparison")
    print("=" * 50)
    
    try:
        # Load pandas results
        print("Loading pandas results...")
        pandas_original = pd.read_parquet("../results/pandas_original.parquet")
        pandas_transformed = pd.read_parquet("../results/pandas_transformed.parquet")  
        pandas_inversed = pd.read_parquet("../results/pandas_inversed.parquet")
        
        # Load Spark results
        print("Loading Spark results...")
        spark_original = pd.read_parquet("../results/spark_original.parquet")
        spark_transformed = pd.read_parquet("../results/spark_transformed.parquet")
        spark_inversed = pd.read_parquet("../results/spark_inversed.parquet")
        
        # Compare original data (should be identical)
        compare_dataframes(pandas_original, spark_original, "Pandas Original", "Spark Original")
        
        # Compare transformed data
        compare_dataframes(pandas_transformed, spark_transformed, "Pandas Transformed", "Spark Transformed")
        
        # Compare inversed data
        compare_dataframes(pandas_inversed, spark_inversed, "Pandas Inversed", "Spark Inversed")
        
    except FileNotFoundError as e:
        print(f"Missing result file: {e}")
        print("Please run pandas and Spark test scripts first:")
        print("  python run_pandas_test.py")
        print("  python run_spark_test.py")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()