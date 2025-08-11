# BD Transformer - Spark Version

A high-performance, distributed data transformation and normalization library built on Apache Spark, designed to handle large-scale datasets efficiently while maintaining data integrity and providing seamless bidirectional transformations.

## Purpose

BD Transformer addresses the critical need for scalable data preprocessing in big data environments. It provides a unified framework for:
- **Data Normalization**: Converting diverse data formats into standardized numerical representations suitable for machine learning
- **Bidirectional Transformation**: Ensuring data can be transformed and recovered with high fidelity
- **Format-Aware Processing**: Intelligently handling formatted strings (currencies, measurements, percentages) while preserving semantic meaning
- **Distributed Computing**: Leveraging Spark's distributed architecture for processing datasets that exceed single-machine memory

## Features

- **Distributed Processing**: Native Spark integration for horizontal scaling across cluster nodes
- **MinMax Normalization**: Efficient distributed computation of min/max statistics with single-pass aggregation
- **Intelligent String Parsing**: Advanced pattern recognition for formatted data (currencies, percentages, units, measurements)
- **Memory-Efficient Operations**: Streaming-based transformations that minimize memory footprint
- **Configurable Pipeline**: YAML-based column-wise transformation settings with validation
- **Bidirectional Transforms**: Lossless inverse transformations to recover original data formats
- **Comprehensive Error Handling**: Detailed validation, logging, and graceful failure recovery
- **Type Safety**: Strong typing support with runtime validation for data integrity

## Design Considerations for Large Datasets

BD Transformer is specifically architected for enterprise-scale data processing:

### **Memory Optimization**
- **Lazy Evaluation**: Leverages Spark's lazy evaluation to minimize memory usage until actions are triggered
- **Column-wise Processing**: Processes columns independently to reduce peak memory requirements
- **Streaming Statistics**: Computes normalization parameters in single-pass operations without materializing intermediate results

### **Distributed Computing Architecture**
- **Partition-Aware Operations**: Distributes computations across Spark partitions for optimal parallelization
- **Broadcast Variables**: Efficiently shares transformation parameters across all worker nodes
- **Fault Tolerance**: Built-in recovery mechanisms handle node failures during processing

### **Scalability Features**
- **Dynamic Resource Management**: Adapts to available cluster resources automatically
- **Incremental Processing**: Supports processing of data in chunks for datasets larger than cluster memory
- **Configurable Batch Sizes**: Tunable parameters for optimal performance across different cluster configurations

### **Performance Optimizations**
- **Catalyst Optimization**: Benefits from Spark's query optimizer for efficient execution plans
- **Columnar Storage Integration**: Optimized for Parquet and other columnar formats
- **Predicate Pushdown**: Minimizes data movement by pushing filters close to data sources

## Installation

```bash
# Install in development mode
pip install -e .

# Or install dependencies directly
pip install pyspark>=3.0.0 pyyaml>=5.4.0 numpy>=1.20.0
```

## Testing and Validation

The library includes comprehensive test suites that validate correctness against pandas implementations and measure performance characteristics. Comparison results and test artifacts are automatically saved in the `tests/` directory:

- `tests/results/`: Contains output parquet files from pandas vs Spark comparisons
- `tests/comparison/`: Comparative analysis scripts and utilities
- `tests/comparison/artifacts/`: Generated test artifacts and performance metrics (excluded from version control)

Run comparison tests to validate transformation accuracy:
```bash
cd tests/comparison
python run_pandas_test.py    # Generate pandas baseline
python run_spark_test.py     # Generate Spark implementation results  
python compare_results.py    # Compare outputs for accuracy
```
