import pandas as pd
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import re
import os
import unicodedata  # Importamos el módulo para normalización de strings

def setup_driver():
    """Configura y retorna el driver de Chrome"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1200")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    return webdriver.Chrome(options=chrome_options)

def normalize_text(text):
    """Normaliza texto para comparación insensible a mayúsculas/acentos"""
    if not isinstance(text, str):
        return ""
    return unicodedata.normalize('NFKD', text.lower()).encode('ascii', errors='ignore').decode('utf-8')

def get_match_logs_table(driver, player_url):
    """Extrae la tabla de registro de partidos para un jugador"""
    try:
        driver.get(player_url)
        time.sleep(3)

        # Aplicar filtro "Todas las competencias"
        try:
            all_comp_btn = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//div[@class='filter']//a[contains(., 'Todas las competencias')]")))
            
            if "current" not in all_comp_btn.find_element(By.XPATH, "./..").get_attribute("class"):
                driver.execute_script("arguments[0].click();", all_comp_btn)
                time.sleep(3)
        except Exception as e:
            print(f"Error al aplicar filtro 'Todas las competencias': {e}")
            return None

        # Encontrar el enlace "Partidos"
        try:
            rows = driver.find_elements(By.CSS_SELECTOR, "table.stats_table tbody tr")
            if not rows:
                print("No se encontraron filas en la tabla")
                return None
            
            last_row = rows[-1]
            cells = last_row.find_elements(By.TAG_NAME, "td")
            if not cells:
                print("No se encontraron celdas en la última fila")
                return None
            
            last_cell = cells[-1]
            match_link = last_cell.find_element(By.TAG_NAME, "a")
            match_url = match_link.get_attribute("href")
            
            if not match_url:
                print("No se encontró enlace de Partidos")
                return None
            
            # Ir a la URL de partidos
            driver.get(match_url)
            time.sleep(3)
            
            # Extraer la tabla de partidos
            html = driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            table = soup.find('table', {'id': 'matchlogs_all'})
            
            if not table:
                print("No se encontró la tabla de partidos")
                return None
            
            # Procesar la tabla
            headers = [th.text.strip() for th in table.find('thead').find_all('tr')[-1].find_all(['th', 'td'])]
            data = []
            
            for row in table.find('tbody').find_all('tr'):
                row_data = [cell.text.strip() for cell in row.find_all(['th', 'td'])]
                if len(row_data) == len(headers):
                    data.append(row_data)
            
            return pd.DataFrame(data, columns=headers) if data else None
            
        except Exception as e:
            print(f"Error al encontrar/enlazar a partidos: {e}")
            return None

    except Exception as e:
        print(f"Error general: {e}")
        return None

def save_match_logs(df, player_name, output_dir='partidos_data'):
    """Guarda la tabla de partidos en archivo CSV"""
    if df is None:
        return False
    
    os.makedirs(output_dir, exist_ok=True)
    clean_name = re.sub(r'[^\w\s-]', '', player_name).strip().replace(' ', '_')
    filename = os.path.join(output_dir, f"{clean_name.lower()}_partidos.csv")
    
    try:
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        return True
    except Exception as e:
        print(f"Error guardando archivo: {e}")
        return False

def find_players_in_csv(csv_path, player_names):
    """Busca los jugadores especificados en el CSV y devuelve sus datos"""
    try:
        df = pd.read_csv(csv_path)
        # Normalizar nombres en el DataFrame
        df['nombre_normalizado'] = df['nombre'].apply(normalize_text)
        
        found_players = []
        for name in player_names:
            normalized_name = normalize_text(name)
            # Buscar coincidencias exactas o parciales
            player_data = df[df['nombre_normalizado'] == normalized_name]
            
            if player_data.empty:
                # Si no hay coincidencia exacta, buscar parcial
                player_data = df[df['nombre_normalizado'].str.contains(normalized_name, regex=False)]
            
            if not player_data.empty:
                for _, row in player_data.iterrows():
                    found_players.append((row['nombre'], row['href']))
                    print(f"✔ Jugador encontrado: {row['nombre']}")
            else:
                print(f"⚠ Jugador no encontrado en el CSV: {name}")
        
        return found_players
    
    except Exception as e:
        print(f"\nError al procesar el archivo CSV: {e}")
        return None

def process_specific_players(csv_path, player_names):
    """Procesa solo los jugadores especificados"""
    if not player_names:
        print("No se proporcionaron nombres de jugadores")
        return False
    
    players_data = find_players_in_csv(csv_path, player_names)
    
    if not players_data:
        print("No se encontraron jugadores para procesar")
        return False
    
    driver = setup_driver()
    results = []
    
    for i, (name, url) in enumerate(players_data, 1):
        print(f"\n[{i}/{len(players_data)}] Procesando {name}...")
        full_url = f"https://fbref.com{url}" if not url.startswith('http') else url
        
        match_logs = get_match_logs_table(driver, full_url)
        if match_logs is not None:
            success = save_match_logs(match_logs, name)
            if success:
                results.append((name, "Éxito"))
                print(f"✔ Datos de partidos guardados para {name}")
            else:
                results.append((name, "Error al guardar"))
        else:
            results.append((name, "Error al extraer datos"))
            print(f"✖ No se pudieron extraer datos de partidos para {name}")
        
        time.sleep(2)
    
    driver.quit()
    
    print("\nResumen de extracción:")
    for name, status in results:
        print(f"- {name}: {status}")
    
    return True

if __name__ == "__main__":
    # Configuración
    CSV_PATH = 'jugadores_activos_colombianos.csv'
    
    # Lista de jugadores a procesar (modificar según necesidad)
    JUGADORES_A_PROCESAR =[

        
        "José Luis Chunga"
    ]
    """"
    JUGADORES_A_PROCESAR = [
        "Luis Díaz",
        "Camilo Vargas",
        "Jhon Arias",
        "Jhon Lucumí",
        "James Rodríguez",
        "Daniel Muñoz",
        "Jefferson Lerma",
        "Richard Ríos",
        "Davinson Sánchez",
        "Johan Mojica",
        "Rafael Borré",
        "Mateus Uribe",
        "Carlos Cuesta",
        "Kevin Castaño",
        "Deiver Machado",
        "Jhon Córdoba",
        "Santiago Arias",
        "Cristian Borja",
        "Jorge Carrascal",
        "Juan Camilo Portilla",
        "Yerson Mosquera",
        "Luis Sinisterra",
        "Juan Quintero",
        "Willer Ditta",
        "Álvaro David Montero",
        "Frank Fabra",
        "Juan Cuadrado",
        "Roger Martínez",
        "Yaser Asprilla",
        "Jaminton Campaz",
        "Cucho",
        "Carlos Gómez",
        "Mateo Cassierra",
        "Andrés Felipe Román",
        "Sebastián Gómez",
        "Marino Hinestroza",
        "José Luis Chunga",
        "Kevin Mier",
        "David Ospina"
    ]
    """
    print(f"\nIniciando extracción para {len(JUGADORES_A_PROCESAR)} jugadores específicos...")
    success = process_specific_players(CSV_PATH, JUGADORES_A_PROCESAR)
    
    if success:
        print("\nProceso completado con éxito")
    else:
        print("\nEl proceso encontró errores. Revisa los mensajes anteriores.")