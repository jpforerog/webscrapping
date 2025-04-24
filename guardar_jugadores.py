import os
import pandas as pd
import sqlite3

# ---------------------------------------------------------------------------
# CONFIGURACIÓN DE LA BASE DE DATOS SQLITE
# ---------------------------------------------------------------------------
# Puedes cambiar el nombre si quieres otra base de datos
db_path = "mi_base_de_datos.db"
connection = sqlite3.connect(db_path)
cursor = connection.cursor()
print(f"Conectado a SQLite DB en '{db_path}'.")

# ---------------------------------------------------------------------------
# FUNCIÓN PARA CREAR TABLA E INSERTAR DESDE CSV
# ---------------------------------------------------------------------------
def crear_tabla_desde_csv(csv_path):
    file_name = os.path.basename(csv_path)
    table_name = file_name.replace(".csv", "")

    # Leer el CSV
    df = pd.read_csv(csv_path, encoding='utf-8')
    print(f"\nLeyendo CSV: {csv_path}")
    print(f"Tabla destino: {table_name}")

    # Eliminar tabla si ya existe
    cursor.execute(f"DROP TABLE IF EXISTS '{table_name}'")
    print(f"Tabla '{table_name}' eliminada si existía.")

    # Crear la tabla con columnas tipo TEXT
    columnas_sql = [f"'{col}' TEXT" for col in df.columns]
    create_sql = f"CREATE TABLE '{table_name}' ({', '.join(columnas_sql)})"
    cursor.execute(create_sql)
    print(f"Tabla '{table_name}' creada.")

    # Insertar los datos
    df = df.astype(str)  # Convertir todo a texto
    placeholders = ", ".join(["?"] * len(df.columns))
    insert_sql = f"INSERT INTO '{table_name}' ({', '.join([f'`{col}`' for col in df.columns])}) VALUES ({placeholders})"
    data = list(df.itertuples(index=False, name=None))
    cursor.executemany(insert_sql, data)
    connection.commit()
    print(f"{len(df)} registros insertados en la tabla '{table_name}'.")

# ---------------------------------------------------------------------------
# PROCESAR TODOS LOS CSV DE UNA CARPETA
# ---------------------------------------------------------------------------
carpeta_csv = r"C:/Users/jupaf/Documents/Proyectos_BI/Proyecto Final/Webscraping/partidos_data"

for archivo in os.listdir(carpeta_csv):
    if archivo.lower().endswith(".csv"):
        ruta_csv = os.path.join(carpeta_csv, archivo)
        crear_tabla_desde_csv(ruta_csv)

# ---------------------------------------------------------------------------
# CERRAR CONEXIÓN
# ---------------------------------------------------------------------------
cursor.close()
connection.close()
print("\nConexión a SQLite cerrada.")
