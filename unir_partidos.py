import sqlite3
import re
from collections import defaultdict

def crear_tabla_partidos():
    conn = sqlite3.connect('mi_base_de_datos.db')
    cursor = conn.cursor()

    try:
        # Paso 1: Obtener todas las tablas "_partidos" (excluyendo "jugadores")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tablas_partidos = [
            tabla[0] for tabla in cursor.fetchall()
            if tabla[0].endswith('_partidos') and tabla[0] != 'jugadores'
        ]

        if not tablas_partidos:
            print("No hay tablas de jugadores para combinar.")
            return

        # Paso 2: Recopilar todas las columnas únicas de todas las tablas
        columnas_unicas = defaultdict(set)
        for tabla in tablas_partidos:
            if not re.match(r'^[a-zA-Z0-9_]+$', tabla):
                continue  # Seguridad
            cursor.execute(f"PRAGMA table_info({tabla})")
            for col in cursor.fetchall():
                columnas_unicas[col[1]].add(col[2])  # Nombre y tipo de columna

        # Crear lista de columnas con tipo más común (ej: usar TEXT si hay conflicto)
        columnas_final = []
        for nombre, tipos in columnas_unicas.items():
            tipo = "TEXT" if len(tipos) > 1 else next(iter(tipos))
            columnas_final.append(f'"{nombre}" {tipo}')

        # Paso 3: Crear tabla 'partidos' con todas las columnas detectadas
        cursor.execute('DROP TABLE IF EXISTS partidos')
        create_sql = f'CREATE TABLE partidos ({", ".join(columnas_final)})'
        cursor.execute(create_sql)

        # Paso 4: Insertar datos (manejar columnas faltantes con NULL)
        for tabla in tablas_partidos:
            cursor.execute(f"PRAGMA table_info({tabla})")
            cols_tabla = [col[1] for col in cursor.fetchall()]
            cols_seleccion = [
                f'"{col}"' if col in cols_tabla else 'NULL'
                for col in columnas_unicas.keys()
            ]
            insert_sql = f'''
                INSERT INTO partidos 
                SELECT {", ".join(cols_seleccion)} FROM "{tabla}"
            '''
            cursor.execute(insert_sql)

        conn.commit()
        print(f"Tabla 'partidos' creada con {len(columnas_final)} columnas!")

    except sqlite3.Error as e:
        print(f"Error: {e}")
    finally:
        conn.close()

crear_tabla_partidos()