import os
import sqlite3
import pandas as pd

# Rutas
carpeta_csv = r"C:/Users/jupaf/Documents/Proyectos_BI/Proyecto Final/Webscraping/partidos_data"
db_path = "mi_base_de_datos.db"

# Conectar a SQLite
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Procesar cada archivo CSV
for archivo in os.listdir(carpeta_csv):
    if archivo.endswith("_partidos.csv"):
        ruta_csv = os.path.join(carpeta_csv, archivo)

        # 1. Extraer nombre del jugador
        nombre_jugador = archivo.replace("_partidos.csv", "").replace("_", " ").strip()

        # 2. Obtener el jugador_id
        cursor.execute("SELECT jugador_id FROM jugadores WHERE nombre_completo = ?", (nombre_jugador,))
        resultado = cursor.fetchone()
        if not resultado:
            print(f"❌ Jugador no encontrado en la tabla: {nombre_jugador}")
            continue
        jugador_id = resultado[0]

        # 3. Leer el CSV
        df = pd.read_csv(ruta_csv)

        # 4. Agregar la columna jugador_id
        df["jugador_id"] = jugador_id

        # 5. Guardar en SQLite: tabla con el mismo nombre que el archivo
        nombre_tabla = archivo.replace(".csv", "")
        df.to_sql(nombre_tabla, conn, if_exists="replace", index=False)
        print(f"✅ Tabla actualizada: {nombre_tabla} con jugador_id = {jugador_id}")

conn.close()
