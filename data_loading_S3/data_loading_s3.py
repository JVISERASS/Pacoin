import boto3
import os
import logging
from configparser import ConfigParser
import boto3.session
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

def get_s3_client(aws_profile_name):
    """
    Inicializa y retorna un cliente de S3 usando un perfil de AWS.
    
    :param aws_profile_name: Nombre del perfil de AWS (definido en ~/.aws/credentials)
    :return: Cliente de S3
    """
    try:
        # Crear una sesión de boto3 con el perfil especificado
        session = boto3.Session(profile_name=aws_profile_name)
        # Crear y retornar el cliente de S3
        return session.client('s3')
    except Exception as e:
        logging.error(f"Error al crear el cliente de S3: {e}")
        

def create_s3_bucket(client, config, bucket_name, region):
    """Creates an S3 bucket if it does not already exist."""
    try:
        client.head_bucket(Bucket=bucket_name)  # Check if bucket exists
        logging.info(f"Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':  # Bucket does not exist
            try:
                # For regions other than us-east-1, LocationConstraint must be specified
                if region == "us-east-1":  # Special case for US East (N. Virginia)
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


def upload_file(client, file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket.

    :param client: S3 client object
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified, file_name is used
    :return: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    try:
        client.upload_file(file_name, bucket, object_name)
        logging.info(f"File '{file_name}' uploaded to '{bucket}' as '{object_name}'")
        return True
    except ClientError as e:
        logging.error(f"Failed to upload file '{file_name}': {e}")
        return False

def run_data_loading_s3():
    """Executes the data upload process to AWS S3."""
    try:
        config = load_config('config.ini')
        s3_client = get_s3_client(config["AWS"]["aws_profile_name"])
        region = "eu-south-2"

        data_root = 'data'  # Root directory containing cryptocurrency folders
        if not os.path.exists(data_root):
            raise FileNotFoundError(f"Data directory '{data_root}' does not exist.")

        # Identify cryptocurrency directories (btc, eth, ltc, etc.)
        crypto_dirs = [d for d in os.listdir(data_root) if os.path.isdir(os.path.join(data_root, d))]
        if not crypto_dirs:
            logging.warning("No cryptocurrency directories found in 'data'. Nothing to upload.")
            return
        
        # Create an S3 bucket for the data
        bucket_name = config["AWS"]["bucket_name"]
        create_s3_bucket(s3_client, config, bucket_name, region)

        # Upload each cryptocurrency folder to the S3 bucket
        for crypto_dir in crypto_dirs:
            crypto_path = os.path.join(data_root, crypto_dir)
            for root, _, files in os.walk(crypto_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    logging.info(f"Uploading file '{file_path}' to S3 bucket '{bucket_name}'...")
                    upload_file(s3_client, file_path, bucket_name)
                    logging.info("Data upload completed.")



    except Exception as e:
        logging.error(f"An error occurred during data upload: {e}")

if __name__ == "__main__":
    run_data_loading_s3()
