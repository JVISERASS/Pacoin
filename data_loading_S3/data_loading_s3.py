import boto3
import os
import logging
from configparser import ConfigParser
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def load_config(config_file='config.ini'):
    """Loads configuration from an INI file."""
    config = ConfigParser()
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Configuration file '{config_file}' not found.")
    config.read(config_file)
    return config

def get_s3_client(config):
    """Initializes and returns an AWS S3 client using credentials from the configuration."""
    try:
        return boto3.client(
            's3',
            region_name=config["AWS"]["region"],
            aws_access_key_id=config["AWS"]["access_key"],
            aws_secret_access_key=config["AWS"]["secret_key"]
        )
    except KeyError as e:
        logging.error(f"Missing configuration key: {e}")
        raise

def create_s3_bucket(client, config,bucket_name, region):
    """Creates an S3 bucket if it does not already exist."""
    try:
        client.head_bucket(Bucket=bucket_name)  # Check if bucket exists
        logging.info(f"Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            try:
                if region == config["AWS"]["region"]:
                    client.create_bucket(Bucket=bucket_name)
                else:
                    client.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': region}
                    )
                logging.info(f"Bucket '{bucket_name}' created successfully.")
            except Exception as e:
                logging.error(f"Failed to create bucket '{bucket_name}': {e}")
        else:
            logging.error(f"Error checking bucket '{bucket_name}': {e}")

def upload_file_to_s3(client, file_path, bucket, object_name=None):
    """Uploads a file to an S3 bucket."""
    if object_name is None:
        object_name = os.path.basename(file_path)  # Use the filename as the object name

    try:
        client.upload_file(file_path, bucket, object_name)
        logging.info(f"Successfully uploaded {file_path} to S3://{bucket}/{object_name}")
    except Exception as e:
        logging.error(f"Failed to upload {file_path} to S3://{bucket}/{object_name}: {e}")

def run_data_loading_s3():
    """Executes the data upload process to AWS S3."""
    try:
        config = load_config('config.ini')
        s3_client = get_s3_client(config)
        region = config["AWS"]["region"]

        data_root = 'data'  # Root directory containing cryptocurrency folders
        if not os.path.exists(data_root):
            raise FileNotFoundError(f"Data directory '{data_root}' does not exist.")

        # Identify cryptocurrency directories (btc, eth, ltc, etc.)
        crypto_dirs = [d for d in os.listdir(data_root) if os.path.isdir(os.path.join(data_root, d))]
        if not crypto_dirs:
            logging.warning("No cryptocurrency directories found in 'data'. Nothing to upload.")
            return

        for crypto in crypto_dirs:
            bucket_name = f"crypto-{crypto}"  # Example: "crypto-btc", "crypto-eth"
            create_s3_bucket(s3_client, config, bucket_name, region)

            crypto_path = os.path.join(data_root, crypto)
            data_files = [f for f in os.listdir(crypto_path) if os.path.isfile(os.path.join(crypto_path, f))]
            if not data_files:
                logging.info(f"No files found in '{crypto}'. Skipping...")
                continue

            for data_file in data_files:
                file_path = os.path.join(crypto_path, data_file)
                object_name = f"{crypto}/{data_file}"  # Keep the structure in S3 (e.g., btc/btc.csv)
                upload_file_to_s3(s3_client, file_path, bucket_name, object_name)

    except Exception as e:
        logging.error(f"An error occurred during data upload: {e}")

if __name__ == "__main__":
    run_data_loading_s3()
