from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, when, sum, avg, exp, lit
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from os import path, walk
import time
import logging
import os

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("CryptoIndicators")

# Deshabilitar la dependencia de Hadoop
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["hadoop.home.dir"] = "C:\\hadoop"

# Iniciar Spark Session con logs visibles
spark = SparkSession.builder \
    .appName("Crypto Technical Indicators") \
    .config("spark.ui.showConsoleProgress", "true") \
    .getOrCreate()

# Ajustar nivel de log de Spark
spark.sparkContext.setLogLevel("WARN")

logger.info("Sesión Spark iniciada correctamente")

# Función para calcular SMA
def calculate_sma(df, column_name, periods_list):
    """Calcula el SMA para diferentes periodos"""
    logger.info(f"Calculando SMA para periodos {periods_list}")
    start_time = time.time()
    
    for n in periods_list:
        window_spec = Window.partitionBy("symbol").orderBy("date").rowsBetween(-(n-1), 0)
        df = df.withColumn(f"SMA_{n}_{column_name}", avg(col(column_name)).over(window_spec))
        
    logger.info(f"SMA calculado en {time.time() - start_time:.2f} segundos")
    return df

# Función para calcular EMA
def calculate_ema(df, column_name, periods_list):
    """Calcula el EMA para diferentes periodos"""
    logger.info(f"Calculando EMA para periodos {periods_list}")
    start_time = time.time()
    
    for n in periods_list:
        # Factor de suavizado
        alpha = 2.0 / (n + 1.0)
        
        # Inicializar EMA con SMA para los primeros N períodos
        window_spec_init = Window.partitionBy("symbol").orderBy("date").rowsBetween(-(n-1), 0)
        df = df.withColumn(f"EMA_{n}_{column_name}_init", 
                         when(col("row_num") >= n, avg(col(column_name)).over(window_spec_init))
                         .otherwise(None))
        
        # Calcular EMA recursivamente
        window_spec = Window.partitionBy("symbol").orderBy("date")
        df = df.withColumn(f"EMA_{n}_{column_name}_prev", 
                         lag(f"EMA_{n}_{column_name}_init", 1).over(window_spec))
        
        df = df.withColumn(f"EMA_{n}_{column_name}", 
                         when(col("row_num") < n, col(f"EMA_{n}_{column_name}_init"))
                         .when(col(f"EMA_{n}{column_name}_prev").isNull(), col(f"EMA{n}_{column_name}_init"))
                         .otherwise(
                             (col(column_name) * alpha) + (col(f"EMA_{n}_{column_name}_prev") * (1 - alpha))
                         ))
        
        # Eliminar columnas temporales
        df = df.drop(f"EMA_{n}{column_name}_init", f"EMA{n}_{column_name}_prev")
    
    logger.info(f"EMA calculado en {time.time() - start_time:.2f} segundos")
    return df

# Función para calcular RSI
def calculate_rsi(df, column_name, periods_list):
    """Calcula el RSI para diferentes periodos"""
    logger.info(f"Calculando RSI para periodos {periods_list}")
    start_time = time.time()
    
    # Agregar columna de cambio en precio
    window_spec = Window.partitionBy("symbol").orderBy("date")
    df = df.withColumn("price_change", col(column_name) - lag(col(column_name), 1).over(window_spec))
    
    for n in periods_list:
        # Calcular ganancias y pérdidas
        df = df.withColumn("gain", when(col("price_change") > 0, col("price_change")).otherwise(0))
        df = df.withColumn("loss", when(col("price_change") < 0, -col("price_change")).otherwise(0))
        
        # Calcular promedio de ganancias y pérdidas
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
    
    logger.info(f"RSI calculado en {time.time() - start_time:.2f} segundos")
    return df

# Función para calcular MACD
def calculate_macd(df, column_name, fast_periods=12, slow_periods=26, signal_periods=9):
    """Calcula el MACD con EMA rápido, lento y línea de señal"""
    logger.info(f"Calculando MACD ({fast_periods},{slow_periods},{signal_periods})")
    start_time = time.time()
    
    # Calcular EMAs
    df = calculate_ema(df, column_name, [fast_periods, slow_periods])
    
    # Calcular línea MACD
    df = df.withColumn(f"MACD_line_{column_name}", 
                     col(f"EMA_{fast_periods}{column_name}") - col(f"EMA{slow_periods}_{column_name}"))
    
    # Calcular señal MACD (EMA de la línea MACD)
    alpha = 2.0 / (signal_periods + 1.0)
    window_spec = Window.partitionBy("symbol").orderBy("date")
    window_spec_init = Window.partitionBy("symbol").orderBy("date").rowsBetween(-(signal_periods-1), 0)
    
    # Inicializar la señal con SMA para los primeros N períodos
    df = df.withColumn(f"MACD_signal_{column_name}_init", 
                     when(col("row_num") >= signal_periods + max(fast_periods, slow_periods), 
                         avg(col(f"MACD_line_{column_name}")).over(window_spec_init))
                     .otherwise(None))
    
    # Calcular señal MACD recursivamente
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
    df = df.drop(f"MACD_signal_{column_name}init", f"MACD_signal{column_name}_prev")
    
    logger.info(f"MACD calculado en {time.time() - start_time:.2f} segundos")
    return df

# Función principal para procesar archivos - ajustada para tu estructura de carpetas
def process_crypto_files(base_path, output_path):
    """Procesa todos los archivos CSV en la estructura de directorios adaptada"""
    logger.info(f"Iniciando procesamiento de archivos en {base_path}")
    
    # Periodos para los indicadores
    sma_periods = [10, 50, 100, 200]
    ema_periods = [10, 50, 100, 200] 
    rsi_periods = [14, 21]
    
    # Comprobar si el directorio base existe
    if not path.exists(base_path):
        logger.error(f"El directorio base {base_path} no existe")
        return
    
    # Obtener lista de criptomonedas (directorios de primer nivel)
    crypto_dirs = [d for d in next(walk(base_path))[1] if not d.startswith('.')]
    logger.info(f"Criptomonedas encontradas: {crypto_dirs}")
    
    if not crypto_dirs:
        logger.error(f"No se encontraron directorios de criptomonedas en {base_path}")
        return
    
    # Contar archivos para seguimiento de progreso
    total_files = 0
    for crypto in crypto_dirs:
        crypto_path = path.join(base_path, crypto)
        for root, dirs, files in walk(crypto_path):
            total_files += len([f for f in files if f.endswith('.csv')])
    
    logger.info(f"Se encontraron {total_files} archivos CSV para procesar")
    processed_files = 0
    
    # Procesar cada criptomoneda
    for crypto in crypto_dirs:
        crypto_path = path.join(base_path, crypto)
        logger.info(f"Procesando criptomoneda: {crypto}")
        
        # Recorrer años
        for root, year_dirs, files in walk(crypto_path):
            for year_dir in [d for d in year_dirs if not d.startswith('.')]:
                year_path = path.join(crypto_path, year_dir)
                logger.info(f"Procesando año: {year_dir} en {year_path}")
                
                # Procesar archivos CSV en el directorio de año
                csv_files = [f for f in next(walk(year_path))[2] if f.endswith('.csv')]
                
                for file in csv_files:
                    file_path = path.join(year_path, file)
                    processed_files += 1
                    
                    logger.info(f"[{processed_files}/{total_files}] Procesando archivo: {file_path}")
                    start_file_time = time.time()
                    
                    try:
                        # Leer archivo CSV
                        df = spark.read.csv(file_path, header=True, inferSchema=True)
                        row_count = df.count()
                        col_count = len(df.columns)
                        logger.info(f"Archivo leído correctamente. Filas: {row_count}, Columnas: {col_count}")
                        
                        # Mostrar esquema
                        logger.info(f"Esquema del archivo: {[f.name for f in df.schema.fields]}")
                        
                        # Mostrar primeras filas para verificar estructura
                        logger.info("Muestra de los primeros registros:")
                        df.show(2, truncate=False)
                        
                        # Agregar columna de fecha si no existe
                        if "date" not in [f.name.lower() for f in df.schema.fields]:
                            # Intentar extraer fecha del nombre de archivo
                            file_date = file.split('.')[0]
                            if "_" in file_date:
                                file_date = file_date.split('_')[-1]
                            
                            try:
                                # Intentar diferentes formatos de fecha
                                if len(file_date) == 8:  # YYYYMMDD
                                    file_date = f"{file_date[:4]}-{file_date[4:6]}-{file_date[6:8]}"
                                elif len(year_dir) == 4:  # Usar año del directorio
                                    file_date = f"{year_dir}-01-01"
                                    
                                df = df.withColumn("date", F.to_date(lit(file_date)))
                                logger.info(f"Fecha asignada: {file_date}")
                            except:
                                # Usar el año como parte de la fecha
                                df = df.withColumn("date", F.to_date(lit(f"{year_dir}-01-01"), "yyyy-MM-dd"))
                                logger.info(f"Fecha asignada basada en el año: {year_dir}-01-01")
                        
                        # Verificar columnas requeridas y renombrar si es necesario
                        required_columns = ["symbol", "open", "high", "low", "close", "volume"]
                        df_columns_lower = [f.name.lower() for f in df.schema.fields]
                        
                        # Renombrar columnas si existen con diferente capitalización
                        for req_col in required_columns:
                            if req_col not in df.columns and req_col.lower() in df_columns_lower:
                                actual_col = [f.name for f in df.schema.fields if f.name.lower() == req_col.lower()][0]
                                df = df.withColumnRenamed(actual_col, req_col)
                                logger.info(f"Columna renombrada: {actual_col} -> {req_col}")
                        
                        # Verificar columnas requeridas después de renombrar
                        missing_columns = [col_name for col_name in required_columns if col_name not in df.columns]
                        
                        if missing_columns:
                            logger.warning(f"Columnas no encontradas en {file_path}: {', '.join(missing_columns)}")
                            if "close" in missing_columns:
                                logger.error("Columna 'close' es requerida para calcular indicadores. Omitiendo archivo.")
                                continue
                                
                        # Asegurar que existe una columna symbol si no está presente
                        if "symbol" not in df.columns:
                            symbol_value = f"CRYPTO:{crypto.upper()}USD"
                            df = df.withColumn("symbol", lit(symbol_value))
                            logger.info(f"Columna 'symbol' añadida con valor: {symbol_value}")
                        
                        # Agregar número de fila para cálculos consecutivos
                        window_spec = Window.partitionBy("symbol").orderBy("date")
                        df = df.withColumn("row_num", F.row_number().over(window_spec))
                        
                        # Calcular indicadores técnicos
                        price_column = "close"  # Usamos el precio de cierre para los indicadores
                        
                        # SMA
                        df = calculate_sma(df, price_column, sma_periods)
                        
                        # EMA
                        df = calculate_ema(df, price_column, ema_periods)
                        
                        # RSI
                        df = calculate_rsi(df, price_column, rsi_periods)
                        
                        # MACD
                        df = calculate_macd(df, price_column)
                        
                        # Eliminar columna temporal de número de fila
                        df = df.drop("row_num")
                        
                        # Crear directorio de salida
                        output_dir = path.join(output_path, crypto, year_dir)
                        output_file = path.join(output_dir, f"{file.split('.')[0]}_indicators")
                        
                        # Asegurar que el directorio de salida existe
                        if not path.exists(output_dir):
                            spark.sparkContext._jvm.java.io.File(output_dir).mkdirs()
                        
                        logger.info(f"Guardando resultados en: {output_file}")
                        
                        # Dentro de process_crypto_files, modifica la parte de guardado:
                        output_file = path.join(output_dir, f"{file.split('.')[0]}_indicators")  # Sin .csv

                        # Reemplaza la escritura con:
                        df.write.mode("overwrite") \
                            .option("header", "true") \
                            .csv(output_file)

                        # Si el problema persiste, verifica si df está vacío antes de guardar:
                        if df.count() > 0:
                            df.write.mode("overwrite").csv(output_file, header=True)
                        else:
                            logger.warning(f"DataFrame vacío para {file_path}. No se guardará.")
                        
                        processing_time = time.time() - start_file_time
                        logger.info(f"Archivo procesado exitosamente en {processing_time:.2f} segundos")
                        
                    except Exception as e:
                        logger.error(f"Error procesando {file_path}: {str(e)}")
    
    logger.info(f"Procesamiento completado. {processed_files} archivos procesados.")

# Ejecutar el procesamiento con manejo de errores
try:
    base_data_path = "clean_data/data"
    output_data_path = "clean_data/data_with_indicators"
    
    logger.info(f"Verificando directorio base: {base_data_path}")
    if not path.exists(base_data_path):
        logger.error(f"El directorio base {base_data_path} no existe.")
        exit(1)
    
    logger.info("Iniciando procesamiento principal...")
    process_crypto_files(base_data_path, output_data_path)
    
except Exception as e:
    logger.error(f"Error en el procesamiento principal: {str(e)}")
finally:
    logger.info("Deteniendo Spark Session...")
    spark.stop()
    logger.info("Procesamiento finalizado.")