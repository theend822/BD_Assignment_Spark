# Large Dataset Performance Testing

## Testing Approach

### 1. Dataset Generation (80GB Dataset)
- **Script**: `scripts/data_gen_80gb.py`
- **Target Size**: 80GB (~20x the 4GB memory limit)
- **Data Structure**: 350 million rows with mixed data types:
  - `day_of_month`: Integer (1-31)
  - `height`: String format with "cm" suffix (e.g., "175.5cm")
  - `account_balance`: Currency string with "$" prefix (e.g., "$5,432.10")
  - `net_profit`: Currency string with "$" prefix, including negatives
  - `customer_ratings`: String format with "stars" suffix (e.g., "4.2stars")
  - `leaderboard_rank`: Integer (1-100,000)
- **Storage**: Chunked into multiple Parquet files (50M rows per chunk)

### 2. Memory Constraint (4GB Limit)
- **Spark Configuration**: Driver and executor memory limited to 4GB
- **Rationale**: Forces distributed processing and tests memory efficiency
- **Configuration**:
  ```
  spark.driver.memory = 4g
  spark.executor.memory = 4g
  spark.driver.maxResultSize = 2g
  ```

### 3. System Specifications (Mac Mini)
**Hardware Configuration:**
- **CPU**: Apple M4 Chip
- **Memory**: 24GB
- **Storage**: 256GB SSD

**Software Environment:**
- **OS**: macOS Sequoia
- **Python**: 3.12.7
- **PySpark**: 4.0.0
- **Java**: OpenJDK 21

### 4. Resource Monitoring with psutil
**Metrics Collected:**
- **CPU Usage**: Percentage utilization over time
- **Memory Usage**: RAM consumption in GB
- **Disk I/O**: Cumulative read/write operations in GB

**Monitoring Features:**
- Real-time data collection (1-second intervals)
- Thread-based monitoring to avoid interference
- Phase markers for different operations (fit, transform, inverse_transform)
- Automated plot generation with matplotlib

### 5. Visualization and Results
**Generated Outputs:**
- Resource usage plots (CPU, Memory, Disk I/O vs. Time)
- Phase markers showing operation boundaries
- System specifications documentation
- Detailed timing measurements
- Summary statistics

## Directory Structure

```
test_on_large_dataset/
├── README.md                    # This documentation
├── scripts/
│   ├── data_gen_80gb.py        # 80GB dataset generation
│   └── resource_monitor.py     # Resource monitoring and testing
├── data/                       # Generated dataset storage
│   ├── 001.parquet
│   ├── 002.parquet
│   └── ...                     # Chunked parquet files
└── results/                    # Test results and plots
    ├── resource_usage_*.png    # Resource monitoring plots
    ├── system_specs.txt        # Hardware specifications
    └── timing_results.txt      # Operation timing data
```

## Usage Instructions

### Step 1: Generate Large Dataset
```bash
cd scripts/
python data_gen.py
```

### Step 2: Run Performance Test
```bash
python resource_monitor.py
```

### Step 3: Verify Results
- View generated parquet in `data/` directory
- View plots in `resource_monitoring/` directory
- Check timing measurements in `timing_results.txt`
- Review system specifications in `system_specs.txt`



## Test Results

### Dataset Generation
![Data Generation](https://drive.google.com/uc?id=1ygn6vR0E1Hzjez1hvfujOhG3uDZyPxjK)

### Resource Monitoring Results
![Resource Usage Plot](https://drive.google.com/uc?id=1a-l0bGQEqHa75YATz6iupjjq7btmDMAz)


### Execution Results
![Script Execution](https://drive.google.com/uc?id=1XpNUTWUTmTrtwCUiSuleZRfVSq2cILvX)


## Issues Encountered and Solutions

### 1. Import Conflicts Due to Missing Virtual Environment
**Problem:** Multiple BD Transformer versions (pandas and Spark) installed simultaneously caused import conflicts, with Python defaulting to the pandas version even when running Spark code.

**Symptoms:**
```python
ERROR: Converter.fit() takes 2 positional arguments but 3 were given
```

**Root Cause:** Both `bd_transformer` (pandas) and `bd_transformer_spark` packages were installed in the same environment, causing Python to import the wrong version.

**Solution:**
- Uninstalled conflicting pandas version: `pip uninstall bd_transformer -y`
- **Recommendation:** Use separate virtual environments for different implementations

### 2. Out-of-Memory Error in inverse_transform()
**Problem:** Original join-based approach for inverse_transform() caused memory exhaustion during row-id joins.

**Symptoms:**
```
org.apache.spark.memory.SparkOutOfMemoryError: Unable to acquire 65536 bytes of memory
```

**Root Cause:** Multiple DataFrame joins with `row_id` required extensive shuffling and memory allocation.

**Original Implementation Issues:**
- Created `row_id` column with `monotonically_increasing_id()`
- Performed separate joins for each column transformation
- Multiple intermediate DataFrame materializations

**Optimization Solution:**
- **Expression-Based Processing:** Return tuples of Spark expressions instead of DataFrames
- **Single Select Operation:** Process all columns in one `select()` statement
- **Eliminated Joins:** Removed row_id dependency and join operations
- **Lazy Evaluation:** Allow Spark's Catalyst optimizer to process entire pipeline efficiently

**Code Changes:**
```python
# Before: Multiple joins approach
def inverse_transform(self, data):
    result_df = data.withColumn("row_id", monotonically_increasing_id())
    for column_name in self.config:
        # Separate processing and joining for each column
        result_df = result_df.join(processed_column, on="row_id")

# After: Expression-based approach  
def inverse_transform(self, data):
    col_list = []
    for column_name in self.config:
        data_expr, valid_expr, error_expr = self.normalizers[column_name].inverse_normalize(column_name)
        data_expr, valid_expr, error_expr = self.converters[column_name].inverse_convert(data_expr, valid_expr, error_expr)
        col_list.extend([data_expr.alias(f"{column_name}_data"), valid_expr.alias(f"{column_name}_valid"), error_expr.alias(f"{column_name}_error")])
    return data.select(*col_list)
```