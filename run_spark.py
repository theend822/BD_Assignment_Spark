import argparse

from pyspark.sql import SparkSession
import yaml

from bd_transformer.transformer import Transformer


def run(config_path: str, data_path: str):
    # Initialize Spark
    spark = SparkSession.builder.appName("BD_Transformer_Spark").getOrCreate()
    
    data = spark.read.parquet(data_path)
    data.show(5)
    
    with open(config_path, "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    
    transformer = Transformer(config)
    transformer.fit(data)

    transformed = transformer.transform(data)
    transformed.show(5)

    inversed = transformer.inverse_transform(transformed)
    inversed.show(5)
    
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config_path", type=str, required=False, default="config/config.yaml"
    )
    parser.add_argument("--data_path", type=str, required=False, default="data/001.parquet")
    args = parser.parse_args()
    run(args.config_path, args.data_path)