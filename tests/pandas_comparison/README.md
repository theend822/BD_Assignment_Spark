# BD Transformer - Pandas vs Spark Comparison

This directory contains comprehensive tests comparing the pandas and Spark implementations of the BD Transformer library.

## Test Dataset

- **Source**: `001.parquet` (10,000 rows × 6 columns)
- **Size**: 296KB file, ~2.32MB in memory
- **Columns**:
  - `day_of_month`: Pure numeric (int64)
  - `height`: Numeric with suffix ("75.03cm")
  - `account_balance`: Currency with prefix ("$X.XX")
  - `net_profit`: Currency, can be negative
  - `customer_ratings`: Decimal with suffix ("3.95stars") 
  - `leaderboard_rank`: Large integers

## Directory Structure

```
tests/
├── comparison/           # Test scripts
│   ├── run_pandas_test.py    # Test pandas version
│   ├── run_spark_test.py     # Test Spark version  
│   └── compare_results.py    # Compare results
├── results/             # Generated comparison data
│   ├── pandas_original.parquet
│   ├── pandas_transformed.parquet
│   ├── pandas_inversed.parquet
│   ├── spark_original.parquet
│   ├── spark_transformed.parquet
│   └── spark_inversed.parquet
└── README.md           # This file
```

## Running the Tests

### Prerequisites
```bash
# Install both versions
cd ../bd_transformer_original && pip install -e .
cd ../bd_transformer_spark && pip install -e .
```

### Execute Tests
```bash
# Run from tests/comparison/ directory
cd tests/comparison

# 1. Test pandas version
python run_pandas_test.py

# 2. Test Spark version  
python run_spark_test.py

# 3. Compare results
python compare_results.py
```

## Comparison Results
![Screenshot](https://drive.google.com/uc?export=view&id=1oNYgD1sq4VMauLA5X1DK94ymOlzQGUCk)
```
🔍 BD Transformer Results Comparison
==================================================
Loading pandas results...
Loading Spark results...

=== Comparing Pandas Original vs Spark Original ===
Shape: (10000, 6)
Column 'day_of_month': Identical
Column 'height': Identical
Column 'account_balance': Identical
Column 'net_profit': Identical
Column 'customer_ratings': Identical
Column 'leaderboard_rank': Identical

 Overall: 0/60000 different (0.0000%)
DataFrames are IDENTICAL!

=== Comparing Pandas Transformed vs Spark Transformed ===
Shape: (10000, 6)
Column 'day_of_month': Identical
Column 'height': Identical
Column 'account_balance': Identical
Column 'net_profit': Identical
Column 'customer_ratings': Identical
Column 'leaderboard_rank': Identical

 Overall: 0/60000 different (0.0000%)
DataFrames are IDENTICAL!

=== Comparing Pandas Inversed vs Spark Inversed ===
Shape: (10000, 6)
Column 'day_of_month': Identical
Column 'height': Identical
Column 'account_balance': Identical
Column 'net_profit': Identical
Column 'customer_ratings': Identical
Column 'leaderboard_rank': Identical

 Overall: 0/60000 different (0.0000%)
DataFrames are IDENTICAL!
```
