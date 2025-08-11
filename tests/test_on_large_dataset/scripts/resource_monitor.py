import argparse
import time
import os
import sys
import threading
from datetime import datetime
import psutil
import matplotlib.pyplot as plt
import numpy as np
import yaml
from pyspark.sql import SparkSession
from pyspark import SparkConf

# Add parent directory to path to import bd_transformer
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../'))
from bd_transformer.transformer import Transformer


class ResourceMonitor:
    def __init__(self, output_dir="results"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        self.cpu_data = []
        self.memory_data = []
        self.disk_io_data = []
        self.timestamps = []
        self.monitoring = False
        self.monitor_thread = None
        
        # Get initial disk I/O stats
        self.initial_disk_io = psutil.disk_io_counters()
        
    def start_monitoring(self):
        """Start resource monitoring in a separate thread"""
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_resources)
        self.monitor_thread.start()
        print("Resource monitoring started...")
        
    def stop_monitoring(self):
        """Stop resource monitoring"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join()
        print("Resource monitoring stopped.")
        
    def _monitor_resources(self):
        """Monitor system resources every second"""
        while self.monitoring:
            # CPU usage (percentage)
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # Memory usage
            memory = psutil.virtual_memory()
            memory_used_gb = memory.used / (1024**3)  # Convert to GB
            
            # Disk I/O
            disk_io = psutil.disk_io_counters()
            if self.initial_disk_io:
                read_bytes = (disk_io.read_bytes - self.initial_disk_io.read_bytes) / (1024**3)  # GB
                write_bytes = (disk_io.write_bytes - self.initial_disk_io.write_bytes) / (1024**3)  # GB
                total_io = read_bytes + write_bytes
            else:
                total_io = 0
                
            # Store data
            self.timestamps.append(datetime.now())
            self.cpu_data.append(cpu_percent)
            self.memory_data.append(memory_used_gb)
            self.disk_io_data.append(total_io)
            
    def plot_resources(self, phase_markers=None):
        """Create resource usage plots"""
        if not self.timestamps:
            print("No monitoring data available")
            return
            
        # Convert timestamps to relative seconds
        start_time = self.timestamps[0]
        time_seconds = [(t - start_time).total_seconds() for t in self.timestamps]
        
        # Create subplots
        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 10))
        
        # CPU Usage
        ax1.plot(time_seconds, self.cpu_data, 'b-', linewidth=1)
        ax1.set_ylabel('CPU Usage (%)')
        ax1.set_title('System Resource Usage During BD Transformer Operations')
        ax1.grid(True, alpha=0.3)
        ax1.set_ylim(0, 100)
        
        # Memory Usage
        ax2.plot(time_seconds, self.memory_data, 'r-', linewidth=1)
        ax2.set_ylabel('Memory Usage (GB)')
        ax2.grid(True, alpha=0.3)
        
        # Disk I/O
        ax3.plot(time_seconds, self.disk_io_data, 'g-', linewidth=1)
        ax3.set_ylabel('Cumulative Disk I/O (GB)')
        ax3.set_xlabel('Time (seconds)')
        ax3.grid(True, alpha=0.3)
        
        # Add phase markers if provided
        if phase_markers:
            colors = ['orange', 'purple', 'brown']
            for i, (phase_name, phase_time) in enumerate(phase_markers.items()):
                relative_time = (phase_time - start_time).total_seconds()
                for ax in [ax1, ax2, ax3]:
                    ax.axvline(x=relative_time, color=colors[i % len(colors)], 
                              linestyle='--', alpha=0.7, label=phase_name)
        
        # Add legend to first subplot
        if phase_markers:
            ax1.legend(loc='upper right')
            
        plt.tight_layout()
        
        # Save plot
        plot_path = os.path.join(self.output_dir, f'resource_usage_{datetime.now().strftime("%Y%m%d_%H%M%S")}.png')
        plt.savefig(plot_path, dpi=300, bbox_inches='tight')
        plt.show()
        
        print(f"Resource usage plot saved to: {plot_path}")
        
        # Print summary statistics
        print("\n=== Resource Usage Summary ===")
        print(f"Average CPU Usage: {np.mean(self.cpu_data):.1f}%")
        print(f"Max CPU Usage: {np.max(self.cpu_data):.1f}%")
        print(f"Average Memory Usage: {np.mean(self.memory_data):.1f} GB")
        print(f"Max Memory Usage: {np.max(self.memory_data):.1f} GB")
        print(f"Total Disk I/O: {max(self.disk_io_data):.1f} GB")
        

def get_system_specs():
    """Get system specifications"""
    specs = {
        'cpu_count': psutil.cpu_count(logical=False),
        'cpu_count_logical': psutil.cpu_count(logical=True),
        'total_memory_gb': psutil.virtual_memory().total / (1024**3),
        'available_memory_gb': psutil.virtual_memory().available / (1024**3),
        'disk_usage': psutil.disk_usage('/'),
        'python_version': sys.version,
    }
    
    # Try to get CPU info (platform-specific)
    try:
        with open('/proc/cpuinfo', 'r') as f:
            cpu_info = f.read()
            for line in cpu_info.split('\n'):
                if 'model name' in line:
                    specs['cpu_model'] = line.split(':')[1].strip()
                    break
    except:
        specs['cpu_model'] = "Unable to determine"
    
    return specs


def setup_spark_session(memory_limit="4g"):
    """Setup Spark session with memory constraints"""
    conf = SparkConf() \
        .setAppName("BD_Transformer_Large_Dataset_Test") \
        .set("spark.driver.memory", memory_limit) \
        .set("spark.executor.memory", memory_limit) \
        .set("spark.driver.maxResultSize", "2g") \
        .set("spark.sql.adaptive.enabled", "true") \
        .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def run_transformation_test(data_path, config_path, memory_limit="4g", output_dir="results"):
    """Run the transformation test with resource monitoring"""
    
    print("=== BD Transformer Large Dataset Test ===")
    print(f"Data path: {data_path}")
    print(f"Config path: {config_path}")
    print(f"Memory limit: {memory_limit}")
    print(f"Output directory: {output_dir}")
    
    # Print system specs
    specs = get_system_specs()
    print("\n=== System Specifications ===")
    for key, value in specs.items():
        print(f"{key}: {value}")
    
    # Save specs to file
    with open(os.path.join(output_dir, "system_specs.txt"), "w") as f:
        f.write("=== System Specifications ===\n")
        for key, value in specs.items():
            f.write(f"{key}: {value}\n")
    
    # Initialize resource monitor
    monitor = ResourceMonitor(output_dir)
    
    # Setup Spark
    spark = setup_spark_session(memory_limit)
    
    # Load configuration
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Initialize transformer
    transformer = Transformer(config)
    
    # Phase timing
    phase_markers = {}
    timings = {}
    
    try:
        # Start monitoring
        monitor.start_monitoring()
        test_start = datetime.now()
        
        print("\n=== Loading Data ===")
        load_start = datetime.now()
        df = spark.read.parquet(data_path)
        print(f"Data loaded. Shape: {df.count()} rows, {len(df.columns)} columns")
        load_time = (datetime.now() - load_start).total_seconds()
        timings['data_loading'] = load_time
        
        print("\n=== Phase 1: fit() ===")
        fit_start = datetime.now()
        phase_markers['fit_start'] = fit_start
        
        transformer.fit(df)
        
        fit_end = datetime.now()
        fit_time = (fit_end - fit_start).total_seconds()
        timings['fit'] = fit_time
        print(f"fit() completed in {fit_time:.2f} seconds")
        
        print("\n=== Phase 2: transform() ===")
        transform_start = datetime.now()
        phase_markers['transform_start'] = transform_start
        
        transformed_df = transformer.transform(df)
        # Force evaluation
        count = transformed_df.count()
        
        transform_end = datetime.now()
        transform_time = (transform_end - transform_start).total_seconds()
        timings['transform'] = transform_time
        print(f"transform() completed in {transform_time:.2f} seconds")
        print(f"Transformed data has {count} rows")
        
        print("\n=== Phase 3: inverse_transform() ===")
        inverse_start = datetime.now()
        phase_markers['inverse_start'] = inverse_start
        
        recovered_df = transformer.inverse_transform(transformed_df)
        # Force evaluation
        count = recovered_df.count()
        
        inverse_end = datetime.now()
        inverse_time = (inverse_end - inverse_start).total_seconds()
        timings['inverse_transform'] = inverse_time
        print(f"inverse_transform() completed in {inverse_time:.2f} seconds")
        print(f"Recovered data has {count} rows")
        
        total_time = (datetime.now() - test_start).total_seconds()
        timings['total'] = total_time
        
        print("\n=== Timing Summary ===")
        for phase, time_taken in timings.items():
            print(f"{phase}: {time_taken:.2f} seconds")
            
        # Save timing results
        with open(os.path.join(output_dir, "timing_results.txt"), "w") as f:
            f.write("=== Timing Results ===\n")
            for phase, time_taken in timings.items():
                f.write(f"{phase}: {time_taken:.2f} seconds\n")
        
    except Exception as e:
        print(f"Error during transformation test: {e}")
        raise
    finally:
        # Stop monitoring and generate plots
        time.sleep(2)  # Allow final measurements
        monitor.stop_monitoring()
        
        # Generate plots
        monitor.plot_resources(phase_markers)
        
        # Cleanup
        spark.stop()
        
        return timings


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run BD Transformer test on large dataset with resource monitoring')
    parser.add_argument('--data_path', type=str, default='../data/', 
                       help='Path to the large dataset directory')
    parser.add_argument('--config_path', type=str, default='../../../config/config.yaml',
                       help='Path to the configuration file')
    parser.add_argument('--memory_limit', type=str, default='4g',
                       help='Spark memory limit (e.g., 4g, 8g)')
    parser.add_argument('--output_dir', type=str, default='../results/',
                       help='Directory to save results')
    
    args = parser.parse_args()
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Run the test
    try:
        timings = run_transformation_test(
            data_path=args.data_path,
            config_path=args.config_path,
            memory_limit=args.memory_limit,
            output_dir=args.output_dir
        )
        print("\n=== Test completed successfully ===")
    except Exception as e:
        print(f"\n=== Test failed: {e} ===")
        sys.exit(1)