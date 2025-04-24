import os
import sqlite3
import pandas as pd

# Ruta de tu carpeta con los CSVs
carpeta_csv = r"C:/Users/jupaf/Documents/Proyectos_BI/Proyecto Final/Webscraping/partidos_data"

# Ruta de tu archivo SQLite
db_path = "mi_base_de_datos.db"

# Conectar o crear la base de datos SQLite
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Crear la tabla jugadores (si no existe ya)
cursor.execute("""
CREATE TABLE IF NOT EXISTS jugadores (
    jugador_id INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre_completo TEXT UNIQUE
)
""")

# Extraer nombres de jugadores desde los nombres de archivo
nombres_jugadores = []
for archivo in os.listdir(carpeta_csv):
    if archivo.endswith("_partidos.csv"):
        nombre = archivo.replace("_partidos.csv", "").replace("_", " ").strip()
        nombres_jugadores.append((nombre,))  # como tupla

# Insertar en la tabla (evita duplicados con INSERT OR IGNORE)
cursor.executemany("""
    INSERT OR IGNORE INTO jugadores (nombre_completo)
    VALUES (?)
""", nombres_jugadores)

conn.commit()

# Mostrar la tabla para verificar
df_jugadores = pd.read_sql_query("SELECT * FROM jugadores", conn)
print(df_jugadores)

conn.close()
