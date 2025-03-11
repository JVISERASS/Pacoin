from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, when, avg, lit
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from os import path, walk
import os
from configparser import ConfigParser

def load_config(config_file='config.ini'):
    """Carga la configuración desde un archivo INI."""
    config = ConfigParser()
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Configuration file '{config_file}' not found.")
    config.read(config_file)
    return config

# Iniciar Spark Session
spark = SparkSession.builder \
    .appName("Crypto Technical Indicators") \
    .getOrCreate()

# Función para calcular SMA
def calculate_sma(df, column_name, periods_list):
    """Calcula el SMA para diferentes periodos usando 'datetime' para ordenar"""
    for n in periods_list:
        window_spec = Window.partitionBy("symbol").orderBy("datetime").rowsBetween(-(n-1), 0)
        df = df.withColumn(f"SMA_{n}_{column_name}", avg(col(column_name)).over(window_spec))
    return df

# Función para calcular EMA
def calculate_ema(df, column_name, periods_list):
    """Calcula el EMA para diferentes periodos usando 'datetime' para ordenar"""
    for n in periods_list:
        alpha = 2.0 / (n + 1.0)
        
        # Inicializar EMA con SMA para los primeros N períodos
        window_spec_init = Window.partitionBy("symbol").orderBy("datetime").rowsBetween(-(n-1), 0)
        df = df.withColumn(f"EMA_{n}_{column_name}_init",
                           when(col("row_num") >= n,
                                avg(col(column_name)).over(window_spec_init))
                           .otherwise(None))
        
        # Calcular EMA recursivamente
        window_spec = Window.partitionBy("symbol").orderBy("datetime")
        df = df.withColumn(f"EMA_{n}_{column_name}_prev",
                           lag(f"EMA_{n}_{column_name}_init", 1).over(window_spec))
        
        df = df.withColumn(f"EMA_{n}_{column_name}",
                           when(col("row_num") < n, col(f"EMA_{n}_{column_name}_init"))
                           .when(col(f"EMA_{n}_{column_name}_prev").isNull(), 
                                 col(f"EMA_{n}_{column_name}_init"))
                           .otherwise(
                               (col(column_name) * alpha) + 
                               (col(f"EMA_{n}_{column_name}_prev") * (1 - alpha))
                           ))
        
        df = df.drop(f"EMA_{n}_{column_name}_init", f"EMA_{n}_{column_name}_prev")
    
    return df

# Función para calcular RSI
def calculate_rsi(df, column_name, periods_list):
    """Calcula el RSI para diferentes periodos usando 'datetime' para ordenar"""
    window_spec = Window.partitionBy("symbol").orderBy("datetime")
    df = df.withColumn("price_change", col(column_name) - lag(col(column_name), 1).over(window_spec))
    
    for n in periods_list:
        df = df.withColumn("gain", when(col("price_change") > 0, col("price_change")).otherwise(0))
        df = df.withColumn("loss", when(col("price_change") < 0, -col("price_change")).otherwise(0))
        
        window_avg = Window.partitionBy("symbol").orderBy("datetime").rowsBetween(-(n-1), 0)
        df = df.withColumn(f"avg_gain_{n}", avg("gain").over(window_avg))
        df = df.withColumn(f"avg_loss_{n}", avg("loss").over(window_avg))
        
        df = df.withColumn(f"rs_{n}", 
                           when(col(f"avg_loss_{n}") == 0, 100)
                           .otherwise(col(f"avg_gain_{n}") / col(f"avg_loss_{n}")))
        
        df = df.withColumn(f"RSI_{n}_{column_name}",
                           when(col(f"avg_loss_{n}") == 0, 100)
                           .otherwise(100 - (100 / (1 + col(f"rs_{n}")))))
        
        df = df.drop(f"rs_{n}", f"avg_gain_{n}", f"avg_loss_{n}")
    
    df = df.drop("gain", "loss", "price_change")
    
    return df

# Función para calcular MACD
def calculate_macd(df, column_name, fast_periods=12, slow_periods=26, signal_periods=9):
    """Calcula el MACD con EMA rápido, lento y línea de señal usando 'datetime' para ordenar"""
    df = calculate_ema(df, column_name, [fast_periods, slow_periods])
    
    df = df.withColumn(f"MACD_line_{column_name}",
                       col(f"EMA_{fast_periods}_{column_name}") - col(f"EMA_{slow_periods}_{column_name}"))
    
    alpha = 2.0 / (signal_periods + 1.0)
    window_spec = Window.partitionBy("symbol").orderBy("datetime")
    window_spec_init = Window.partitionBy("symbol").orderBy("datetime").rowsBetween(-(signal_periods-1), 0)
    
    df = df.withColumn(f"MACD_signal_{column_name}_init",
                       when(col("row_num") >= signal_periods + max(fast_periods, slow_periods),
                            avg(col(f"MACD_line_{column_name}")).over(window_spec_init))
                       .otherwise(None))
    
    df = df.withColumn(f"MACD_signal_{column_name}_prev",
                       lag(f"MACD_signal_{column_name}_init", 1).over(window_spec))
    
    df = df.withColumn(f"MACD_signal_{column_name}",
                       when(col("row_num") < signal_periods + max(fast_periods, slow_periods),
                            col(f"MACD_signal_{column_name}_init"))
                       .when(col(f"MACD_signal_{column_name}_prev").isNull(),
                             col(f"MACD_signal_{column_name}_init"))
                       .otherwise(
                           (col(f"MACD_line_{column_name}") * alpha) + 
                           (col(f"MACD_signal_{column_name}_prev") * (1 - alpha))
                       ))
    
    df = df.withColumn(f"MACD_histogram_{column_name}",
                       col(f"MACD_line_{column_name}") - col(f"MACD_signal_{column_name}"))
    
    df = df.drop(f"MACD_signal_{column_name}_init", f"MACD_signal_{column_name}_prev")
    
    return df

def process_crypto_parquet(base_path, output_path):
    """
    Lee datos en formato Parquet de la estructura de directorios:
       base_path/cripto/2021/
       base_path/cripto/2022/
       ...
    Calcula indicadores técnicos y guarda los resultados.
    """
    # Periodos para los indicadores
    sma_periods = [10, 50, 100, 200]
    ema_periods = [10, 50, 100, 200]
    rsi_periods = [14, 21]

    # Lista de criptos y años (ajusta según tu estructura real)
    cryptos = ["btc", "eth", "ltc", "pepe", "xrp"]
    years = ["2021", "2022", "2023", "2024", "2025"]

    for crypto in cryptos:
        for y in years:
            year_path = path.join(base_path, crypto, y)
            if not path.exists(year_path):
                print(f"Carpeta no encontrada: {year_path}")
                continue
            
            print(f"Buscando archivos Parquet en: {year_path}")
            
            parquet_files = []
            for root, dirs, files in os.walk(year_path):
                for file in files:
                    if file.endswith('.parquet'):
                        parquet_files.append(os.path.join(root, file))
            
            if not parquet_files:
                print(f"No se encontraron archivos Parquet en {year_path}")
                continue
                
            print(f"Procesando {len(parquet_files)} archivos Parquet")
            
            try:
                df = spark.read.parquet(*parquet_files)
                
                if "symbol" not in df.columns:
                    df = df.withColumn("symbol", lit(crypto.upper()))
    
                if "datetime" not in df.columns:
                    raise ValueError("La columna 'datetime' es requerida pero no se encontró en los datos.")
    
                required_columns = ["symbol", "open", "high", "low", "close", "volume", "datetime"]
                missing_columns = [col_name for col_name in required_columns if col_name not in df.columns]
                if missing_columns:
                    print(f"Advertencia: Columnas {missing_columns} no encontradas en {year_path}")
                    for col_name in missing_columns:
                        df = df.withColumn(col_name, lit(None).cast("double"))
    
                # Crear la columna 'row_num' usando 'datetime' para el orden
                window_spec = Window.partitionBy("symbol").orderBy("datetime")
                df = df.withColumn("row_num", F.row_number().over(window_spec))
    
                price_column = "close"
                df = calculate_sma(df, price_column, sma_periods)
                df = calculate_ema(df, price_column, ema_periods)
                df = calculate_rsi(df, price_column, rsi_periods)
                df = calculate_macd(df, price_column)
    
                df = df.drop("row_num")
    
                print(f"Mostrando ultimas filas de {crypto.upper()}-{y} con indicadores:")
                print(df.tail(5))
    
                output_dir = path.join(output_path, crypto, y)
                print(f"Guardando resultados en: {output_dir}")
                
                os.makedirs(os.path.dirname(output_dir), exist_ok=True)
                df.write.mode("overwrite").parquet(output_dir)
                
            except Exception as e:
                print(f"Error procesando {year_path}: {str(e)}")
                continue

if __name__ == "__main__":
    try:
        config = load_config()
        base_data_path = config.get('LOCAL', 'clean_parquet_data_dir')
        output_data_path = config.get('LOCAL', 'parquet_data_with_indicators')
        
        process_crypto_parquet(base_data_path, output_data_path)
        
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        spark.stop()
