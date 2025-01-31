import os
from configparser import ConfigParser
from TradingviewData.main import TradingViewData, Interval
import pandas as pd

def load_config(config_file='config.ini'):
    """Load configuration from a file."""
    config = ConfigParser()
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Config file '{config_file}' not found.")
    config.read(config_file)
    return config

def get_cryptos_from_config(config):
    """Get the cryptocurrencies and their exchanges from the config."""
    cryptos = {}
    for crypto in config['Cryptos']:
        cryptos[crypto] = config['Cryptos'][crypto].split(', ')
    return cryptos

def get_download_config(config):
    """Get the download configuration parameters."""
    try:
        interval = Interval(config.get('Ingestion', 'interval'))
        n_downloads = int(config.get('Ingestion', 'nDownloads'))
    except Exception as e:
        raise ValueError(f"Error reading 'interval' or 'nDownloads' from the config: {e}")
    return interval, n_downloads

def download_crypto_data(request, crypto, crypto_types, interval, n_downloads, save_path):
    """Download cryptocurrency data and save to CSV."""
    for crypto_type in crypto_types:
        exchange, symbol = crypto_type.split(":")
        print(f"Downloading data of {symbol} from {exchange} exchange...")
        try:
            data = request.get_hist(symbol, exchange, interval=interval, n_bars=n_downloads)
            print(data.head())
            data.to_csv(f"{save_path}/{crypto}/{crypto_type}.csv")
            print(f"Data for {symbol} downloaded successfully\n")
        except Exception as e:
            print(f"Failed to download data for {symbol} from {exchange}: {e}")

def run_data_ingestion():
    """Main function to execute the data ingestion process."""
    try:
        request = TradingViewData()
        
        config = load_config('config.ini')
        cryptos = get_cryptos_from_config(config)
        
        print("Cryptos to download: ")
        for crypto in cryptos:
            print(f"{crypto}: {cryptos[crypto]}")
        
        save_path = config.get('Ingestion', 'save_path')
        os.makedirs(save_path, exist_ok=True)
        
        interval, n_downloads = get_download_config(config)
        
        for crypto, crypto_types in cryptos.items():
            os.makedirs(f"{save_path}/{crypto}", exist_ok=True)
            download_crypto_data(request, crypto, crypto_types, interval, n_downloads, save_path)
        
        print("All data downloaded successfully")

    except Exception as e:
        print(f"Error: {e}")
        exit(1)

if __name__ == '__main__':
    run_data_ingestion()