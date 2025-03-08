from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, when, sum, avg, lit
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from os import path, walk
import os
from configparser import ConfigParser


def load_config(config_file='config.ini'):
    """Loads the configuration from an INI file."""
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
    """Calcula el SMA para diferentes periodos"""
    for n in periods_list:
        window_spec = Window.partitionBy("symbol").orderBy("date").rowsBetween(-(n-1), 0)
        df = df.withColumn(f"SMA_{n}_{column_name}", avg(col(column_name)).over(window_spec))
    return df

# Función para calcular EMA
def calculate_ema(df, column_name, periods_list):
    """Calcula el EMA para diferentes periodos"""
    for n in periods_list:
        # Factor de suavizado
        alpha = 2.0 / (n + 1.0)
        
        # Inicializar EMA con SMA para los primeros N períodos
        window_spec_init = Window.partitionBy("symbol").orderBy("date").rowsBetween(-(n-1), 0)
        df = df.withColumn(f"EMA_{n}_{column_name}_init",
                           when(col("row_num") >= n,
                                avg(col(column_name)).over(window_spec_init))
                           .otherwise(None))
        
        # Calcular EMA recursivamente
        window_spec = Window.partitionBy("symbol").orderBy("date")
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
        
        # Eliminar columnas temporales
        df = df.drop(f"EMA_{n}_{column_name}_init", f"EMA_{n}_{column_name}_prev")
    
    return df

# Función para calcular RSI
def calculate_rsi(df, column_name, periods_list):
    """Calcula el RSI para diferentes periodos"""
    # Agregar columna de cambio en precio
    window_spec = Window.partitionBy("symbol").orderBy("date")
    df = df.withColumn("price_change", col(column_name) - lag(col(column_name), 1).over(window_spec))
    
    for n in periods_list:
        # Calcular ganancias y pérdidas
        df = df.withColumn("gain", when(col("price_change") > 0, col("price_change")).otherwise(0))
        df = df.withColumn("loss", when(col("price_change") < 0, -col("price_change")).otherwise(0))
        
        # Calcular promedio de ganancias y pérdidas
        window_avg = Window.partitionBy("symbol").orderBy("date").rowsBetween(-(n-1), 0)
        df = df.withColumn(f"avg_gain_{n}", avg("gain").over(window_avg))
        df = df.withColumn(f"avg_loss_{n}", avg("loss").over(window_avg))
        
        # Calcular RS y RSI
        df = df.withColumn(f"rs_{n}", 
                           when(col(f"avg_loss_{n}") == 0, 100)
                           .otherwise(col(f"avg_gain_{n}") / col(f"avg_loss_{n}")))
        
        df = df.withColumn(f"RSI_{n}_{column_name}",
                           when(col(f"avg_loss_{n}") == 0, 100)
                           .otherwise(100 - (100 / (1 + col(f"rs_{n}")))))
        
        # Eliminar columnas temporales
        df = df.drop(f"rs_{n}", f"avg_gain_{n}", f"avg_loss_{n}")
    
    # Eliminar columnas temporales
    df = df.drop("gain", "loss", "price_change")
    
    return df

# Función para calcular MACD
def calculate_macd(df, column_name, fast_periods=12, slow_periods=26, signal_periods=9):
    """Calcula el MACD con EMA rápido, lento y línea de señal"""
    # Calcular EMAs
    df = calculate_ema(df, column_name, [fast_periods, slow_periods])
    
    # Calcular línea MACD
    df = df.withColumn(f"MACD_line_{column_name}",
                       col(f"EMA_{fast_periods}_{column_name}") - col(f"EMA_{slow_periods}_{column_name}"))
    
    # Calcular señal MACD (EMA de la línea MACD)
    alpha = 2.0 / (signal_periods + 1.0)
    window_spec = Window.partitionBy("symbol").orderBy("date")
    window_spec_init = Window.partitionBy("symbol").orderBy("date").rowsBetween(-(signal_periods-1), 0)
    
    # Inicializar la señal con SMA para los primeros N períodos
    df = df.withColumn(f"MACD_signal_{column_name}_init",
                       when(col("row_num") >= signal_periods + max(fast_periods, slow_periods),
                            avg(col(f"MACD_line_{column_name}")).over(window_spec_init))
                       .otherwise(None))
    
    # Calcular señal MACD recursivamente
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
    
    # Calcular histograma MACD
    df = df.withColumn(f"MACD_histogram_{column_name}",
                       col(f"MACD_line_{column_name}") - col(f"MACD_signal_{column_name}"))
    
    # Eliminar columnas temporales
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
            # Carpeta base del año
            year_path = path.join(base_path, crypto, y)
            
            # Verifica si existe la carpeta
            if not path.exists(year_path):
                print(f"Carpeta no encontrada: {year_path}")
                continue
            
            print(f"Buscando archivos Parquet en: {year_path}")
            
            # Buscar todos los archivos Parquet en subdirectorios
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
                # Lee todos los archivos parquet encontrados
                df = spark.read.parquet(*parquet_files)
                
                # Asegurarnos de tener una columna 'symbol'
                if "symbol" not in df.columns:
                    df = df.withColumn("symbol", lit(crypto.upper()))
    
                # Asegurarnos de tener una columna 'date'
               # Asegurarnos de tener la columna 'date'
                if "date" not in df.columns:
                    windowSpec = Window.partitionBy(F.lit(1)).orderBy(F.monotonically_increasing_id())
                    df = df.withColumn("row_num", F.row_number().over(windowSpec) - 1)
                    df = df.withColumn("date", F.expr(f"date_add('{y}-01-01', row_num)")).drop("row_num")
                # Verificar columnas requeridas
                required_columns = ["symbol", "open", "high", "low", "close", "volume"]
                missing_columns = [col_name for col_name in required_columns if col_name not in df.columns]
                if missing_columns:
                    print(f"Advertencia: Columnas {missing_columns} no encontradas en {year_path}")
                    # Añadir columnas faltantes con valores nulos si es necesario
                    for col_name in missing_columns:
                        df = df.withColumn(col_name, lit(None).cast("double"))
    
                # Crear la columna 'row_num' para poder hacer cálculos consecutivos
                window_spec = Window.partitionBy("symbol").orderBy("date")
                df = df.withColumn("row_num", F.row_number().over(window_spec))
    
                # Calcular indicadores técnicos usando el precio de cierre
                price_column = "close"
    
                # SMA
                df = calculate_sma(df, price_column, sma_periods)
    
                # EMA
                df = calculate_ema(df, price_column, ema_periods)
    
                # RSI
                df = calculate_rsi(df, price_column, rsi_periods)
    
                # MACD
                df = calculate_macd(df, price_column)
    
                # Eliminar columna temporal 'row_num'
                df = df.drop("row_num")
    
                # MOSTRAR UN HEAD (df.show) PARA VER LOS INDICADORES
                print(f"Mostrando primeras filas de {crypto.upper()}-{y} con indicadores:")
                print(df.tail(1)) # Muestra las primeras 5 filas
    
                # Guardar resultados en Parquet
                output_dir = path.join(output_path, crypto, y)
                print(f"Guardando resultados en: {output_dir}")
                
                # Crear directorio si no existe
                os.makedirs(os.path.dirname(output_dir), exist_ok=True)
                
                df.write.mode("overwrite").parquet(output_dir)
                
            except Exception as e:
                print(f"Error procesando {year_path}: {str(e)}")
                continue

if __name__ == "__main__":
    try:
        # Cargar configuración
        config = load_config()
        base_data_path = config.get('LOCAL', 'clean_parquet_data_dir')         # Donde están las carpetas btc, eth, etc.
        output_data_path = config.get('LOCAL', 'parquet_data_with_indicators') # Donde se guardarán los resultados
        
        # Ejecutar el procesamiento
        process_crypto_parquet(base_data_path, output_data_path)
        
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        # Detener Spark
        spark.stop()