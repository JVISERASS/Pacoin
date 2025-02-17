import boto3
import os
import logging
from configparser import ConfigParser
import boto3.session
from botocore.exceptions import ClientError
import time

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
        return session.client('glue')
    except Exception as e:
        logging.error(f"Error al crear el cliente de S3: {e}")
        
def create_and_execute_crawler():
    # Configura los valores clave
    CRAWLER_NAME = "s3-to-glue-crawler"
    ROLE_ARN = "arn:aws:iam::585768141030:role/service-role/AWSGlueServiceRole-crawlertest"  # Reemplázalo con tu ARN de IAM
    DATABASE_NAME = "trade_data_imat3b09"
    S3_BUCKET_PATH = "s3://pacoin"

    # Cliente de AWS Glue
    config = load_config('config.ini')
    glue_client = get_s3_client(config["AWS"]["aws_profile_name"])

    # Función para verificar si el crawler ya existe
    def crawler_exists(name):
        try:
            glue_client.get_crawler(Name=name)
            return True
        except glue_client.exceptions.EntityNotFoundException:
            return False

    # Crear el crawler si no existe
    if not crawler_exists(CRAWLER_NAME):
        response = glue_client.create_crawler(
    Name=CRAWLER_NAME,
    Role=ROLE_ARN,
    DatabaseName=DATABASE_NAME,
    Targets={'S3Targets': [
        {'Path': 's3://pacoin/btc/'},
        {'Path': 's3://pacoin/eth/'},
        {'Path': 's3://pacoin/ltc/'},
         {'Path': 's3://pacoin/xrp/'}
    ]},
    TablePrefix='trade_data_',
    SchemaChangePolicy={
        'UpdateBehavior': 'UPDATE_IN_DATABASE',
        'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
    }
)

        print(f"Crawler '{CRAWLER_NAME}' creado exitosamente.")

    # Ejecutar el crawler
    print(f"Ejecutando el crawler '{CRAWLER_NAME}'...")
    glue_client.start_crawler(Name=CRAWLER_NAME)

    # Esperar a que termine el crawler
    while True:
        status = glue_client.get_crawler(Name=CRAWLER_NAME)['Crawler']['State']
        if status == 'READY':
            print(f"Crawler '{CRAWLER_NAME}' finalizado con éxito.")
            break
        print("Esperando a que el crawler termine...")
        time.sleep(10)  # Esperar 10 segundos antes de verificar nuevamente


if __name__ == "__main__":
    create_and_execute_crawler()