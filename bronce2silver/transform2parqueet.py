import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from configparser import ConfigParser

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def convert_csv_to_parquet(spark, csv_file_path, parquet_file_path):
    """Converts a local CSV file to Parquet format and saves it in a local directory."""
    try:
        # Define the schema of the CSV file
        schema = StructType([
            StructField("datetime", StringType(), True),
            StructField("symbol", StringType(), True),
            StructField("open", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("close", DoubleType(), True),
            StructField("volume", DoubleType(), True)
        ])
        
        df = spark.read.csv(csv_file_path, header=True, sep=",", schema=schema)
        
        df.write.parquet(parquet_file_path, mode='overwrite')
        
        logging.info(f"File converted and saved at: {parquet_file_path}")
    except Exception as e:
        logging.error(f"Error converting {csv_file_path} to Parquet: {e}")

def process_all_csv_files(spark, input_dir, output_dir):
    """Processes all CSV files in the input directory and converts them to Parquet in the output directory."""
    try:
        for root, _, files in os.walk(input_dir):
            for file in files:
                if file.endswith(".csv"):
                    csv_file_path = os.path.abspath(os.path.join(root, file))
                    logging.info(f"Processing file: {csv_file_path}")
                    relative_path = os.path.relpath(csv_file_path, input_dir)
                    parquet_file_path = os.path.abspath(os.path.join(output_dir, relative_path.replace('.csv', '.parquet')))
                    
                    # Create output directory if it does not exist
                    os.makedirs(os.path.dirname(parquet_file_path), exist_ok=True)
                    
                    convert_csv_to_parquet(spark, csv_file_path, parquet_file_path)
    except Exception as e:
        logging.error(f"Error processing files in directory {input_dir}: {e}")

def load_config(config_file='config.ini'):
    """Loads the configuration from an INI file."""
    config = ConfigParser()
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Configuration file '{config_file}' not found.")
    config.read(config_file)
    return config

def main():
    config = load_config()
    input_dir = config.get('LOCAL', 'clean_data_dir')
    output_dir = config.get('LOCAL', 'clean_parquet_data_dir')

    spark = SparkSession.builder \
        .appName("CSV to Parquet Conversion") \
        .getOrCreate()

    process_all_csv_files(spark, input_dir, output_dir)

    spark.stop()

if __name__ == "__main__":
    main()




