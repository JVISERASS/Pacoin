import os
import pandas as pd
from typing import Tuple


def missing_data(df: pd.DataFrame) -> Tuple[int, pd.Series]:
    """Calcula el número total y el porcentaje de valores faltantes en el DataFrame."""
    missing = df.isnull().sum()
    total = missing.sum()
    return total, 100 * missing / df.shape[0]


def load_data(path: str) -> pd.DataFrame:
    """Carga los datos desde el archivo CSV especificado."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Archivo '{path}' no encontrado.")

    # Cargar el archivo y asegurar que 'datetime' sea el índice
    df = pd.read_csv(path, parse_dates=['datetime'])
    df.set_index('datetime', inplace=True)

    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia los datos: maneja valores faltantes y aplica un promedio móvil de 7 días."""

    # Identificar columnas numéricas y no numéricas
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    non_numeric_cols = df.select_dtypes(exclude=['float64', 'int64']).columns

    # Convertir las columnas numéricas a tipo float
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

    # Aplicar un promedio móvil de 7 días centrado para rellenar valores NaN en columnas numéricas
    df[numeric_cols] = df[numeric_cols].fillna(
        df[numeric_cols].rolling(window=7, min_periods=1, center=True).mean()
    )

    # Las columnas no numéricas se mantienen sin cambios
    df[non_numeric_cols] = df[non_numeric_cols]

    return df


def process_all_data(base_path: str, clean_path: str):
    """Itera sobre todas las carpetas y archivos CSV para limpiar los datos."""
    # Crear la carpeta clean_data si no existe
    os.makedirs(clean_path, exist_ok=True)

    for root, dirs, files in os.walk(base_path):
        for file in files:
            if file.endswith(".csv"):  # Procesar solo archivos CSV
                file_path = os.path.join(root, file)
                print(f"📂 Procesando archivo: {file_path}")

                try:
                    df = load_data(file_path)

                    total_missing, missing_percent = missing_data(df)
                    print(f"🔍 Total de datos faltantes en '{file}': {total_missing}")
                    print("📉 Porcentaje de datos faltantes por columna:")
                    print(missing_percent)

                    cleaned_df = clean_data(df)

                    print(f"columnames: {cleaned_df.columns}")

                    years = df.index.year

                    for year in years.unique():
                        year_df = cleaned_df[years == year]
                        coin_name = root.split("/")[-1]
                        os.makedirs(os.path.join(clean_path, coin_name, str(year)), exist_ok=True)

                        year_df.to_csv(
                            os.path.join(clean_path, coin_name,str(year),f"{year}_{file.replace(":","")}"),
                            index=False
                        )

                except Exception as e:
                    print(f"❌ Error al procesar {file_path}: {e}")


if __name__ == "__main__":
    base_path = "data"  # Carpeta donde están los CSV originales
    clean_path = "clean_data"  # Carpeta donde se guardarán los datos limpios
    process_all_data(base_path, clean_path)