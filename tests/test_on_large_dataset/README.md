# Large Dataset Performance Testing

This directory contains scripts and results for testing BD Transformer performance on datasets that exceed available system memory (~20x machine RAM).

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
- **CPU**: Apple M2 Chip (8-core CPU)
- **Memory**: 16GB Unified Memory
- **Storage**: 512GB SSD
- **Architecture**: ARM64

**Software Environment:**
- **OS**: macOS Sonoma
- **Python**: 3.11+
- **Spark**: 3.5.0+
- **Java**: OpenJDK 11

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
python data_gen_80gb.py --file_path ../data/ --num_rows 350000000 --chunk_size 50000000
```

### Step 2: Run Performance Test
```bash
python resource_monitor.py \
    --data_path ../data/ \
    --config_path ../../../config/config.yaml \
    --memory_limit 4g \
    --output_dir ../results/
```

### Step 3: Analyze Results
- View generated plots in `results/` directory
- Check timing measurements in `timing_results.txt`
- Review system specifications in `system_specs.txt`

## Expected Performance Characteristics

### Memory Efficiency
- **Lazy Evaluation**: Spark operations should not load entire dataset into memory
- **Streaming Processing**: Data processed in chunks/partitions
- **Memory Usage**: Should stay within 4GB limit despite 80GB dataset

### CPU Utilization
- **Multi-core Usage**: Should utilize available CPU cores efficiently
- **Phase Variations**: Different utilization patterns for fit/transform/inverse operations

### Disk I/O Patterns
- **Sequential Reads**: Efficient parquet file processing
- **Minimal Writes**: Limited intermediate data materialization
- **I/O Optimization**: Leveraging columnar storage benefits

## Key Performance Indicators

### Timing Metrics
1. **fit() Duration**: Time to compute normalization parameters
2. **transform() Duration**: Time to apply transformations
3. **inverse_transform() Duration**: Time to recover original formats
4. **Total Processing Time**: End-to-end operation duration

### Resource Metrics
1. **Peak Memory Usage**: Maximum RAM consumption
2. **Average CPU Usage**: Processing efficiency
3. **Total Disk I/O**: Data movement overhead
4. **Memory Efficiency Ratio**: Processing capability vs. memory constraint

### Scalability Indicators
1. **Data-to-Memory Ratio**: Successfully processing 20x memory size
2. **Partition Utilization**: Effective distributed processing
3. **Resource Stability**: Consistent performance under memory pressure

## Optimization Strategies

### Memory Management
- **Partition Size Tuning**: Optimal chunk sizes for available memory
- **Caching Strategy**: Strategic use of Spark caching for reused data
- **Garbage Collection**: Efficient JVM memory management

### Performance Tuning
- **Adaptive Query Execution**: Dynamic optimization based on data characteristics
- **Columnar Processing**: Leveraging Parquet format benefits
- **Predicate Pushdown**: Minimizing data movement

### Monitoring Enhancements
- **Real-time Alerts**: Memory threshold monitoring
- **Bottleneck Identification**: CPU/I/O constraint analysis
- **Performance Regression Detection**: Baseline comparison capabilities

## Troubleshooting

### Common Issues
1. **OutOfMemoryError**: Reduce partition size or increase memory limits
2. **Slow Performance**: Check disk I/O bottlenecks and partition distribution
3. **Resource Monitoring Gaps**: Ensure psutil compatibility with system

### Performance Tips
1. **SSD Storage**: Use fast storage for better I/O performance
2. **Memory Allocation**: Leave headroom for OS and other processes
3. **Java GC Tuning**: Optimize garbage collection for Spark workloads

## Results Analysis

Results from this testing will demonstrate:
- BD Transformer's ability to handle datasets exceeding system memory
- Resource utilization patterns and efficiency
- Scalability characteristics under memory constraints
- Performance bottlenecks and optimization opportunities

The generated plots and metrics provide insights into the library's distributed processing capabilities and help identify areas for further optimization.