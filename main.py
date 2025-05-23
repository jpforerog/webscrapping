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

def setup_driver():
    """Configura y retorna el driver de Chrome"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1200")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    return webdriver.Chrome(options=chrome_options)

def get_match_logs_table(driver, player_url):
    """Extrae la tabla de registro de partidos para un jugador"""
    try:
        # 1. Ir al perfil del jugador
        driver.get(player_url)
        time.sleep(3)

        # 2. Aplicar filtro "Todas las competencias"
        try:
            all_comp_btn = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//div[@class='filter']//a[contains(., 'Todas las competencias')]")))
            
            if "current" not in all_comp_btn.find_element(By.XPATH, "./..").get_attribute("class"):
                driver.execute_script("arguments[0].click();", all_comp_btn)
                time.sleep(3)
        except Exception as e:
            print(f"Error al aplicar filtro 'Todas las competencias': {e}")
            return None

        # 3. Encontrar el enlace "Partidos" en la última celda de la última fila
        try:
            # Encontrar todas las filas de la tabla
            rows = driver.find_elements(By.CSS_SELECTOR, "table.stats_table tbody tr")
            if not rows:
                print("No se encontraron filas en la tabla")
                return None
            
            # Obtener la última fila
            last_row = rows[-1]
            
            # Encontrar todas las celdas en la última fila
            cells = last_row.find_elements(By.TAG_NAME, "td")
            if not cells:
                print("No se encontraron celdas en la última fila")
                return None
            
            # Obtener la última celda
            last_cell = cells[-1]
            
            # Buscar el enlace "Partidos" dentro de la última celda
            match_link = last_cell.find_element(By.TAG_NAME, "a")
            match_url = match_link.get_attribute("href")
            
            if not match_url:
                print("No se encontró enlace de Partidos")
                return None
            
            # 4. Ir a la URL de partidos
            driver.get(match_url)
            time.sleep(3)
            
            # Extraer la tabla de registro de partidos
            html = driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            table = soup.find('table', {'id': 'matchlogs_all'})
            
            if not table:
                print("No se encontró la tabla de partidos")
                return None
            
            # Procesar la tabla a DataFrame
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
    
    # Crear directorio si no existe
    os.makedirs(output_dir, exist_ok=True)
    
    # Limpiar el nombre del jugador para el nombre de archivo
    clean_name = re.sub(r'[^\w\s-]', '', player_name).strip().replace(' ', '_')
    filename = os.path.join(output_dir, f"{clean_name.lower()}_partidos.csv")
    
    try:
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        return True
    except Exception as e:
        print(f"Error guardando archivo: {e}")
        return False

def process_player_links(csv_path, num_players=10):
    """Procesa los primeros N jugadores del CSV"""
    try:
        df = pd.read_csv(csv_path)
      

        player_links = df['href'].head(num_players).tolist()
        player_names = df['nombre'].head(num_players).tolist()
        
        driver = setup_driver()
        results = []
        
        for i, (url, name) in enumerate(zip(player_links, player_names), 1):
            full_url = f"https://fbref.com{url}" if not url.startswith('http') else url
            print(f"\n[{i}/{num_players}] Procesando {name}...")
            
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
            
            # Pequeña pausa entre jugadores
            time.sleep(2)
        
        driver.quit()
        
        # Mostrar resumen
        print("\nResumen de extracción:")
        for name, status in results:
            print(f"- {name}: {status}")
            
        return True
    
    except Exception as e:
        print(f"\nError al procesar el archivo CSV: {e}")
        print("Asegúrate que el CSV tiene las columnas 'nombre' y 'href'")
        return False

if __name__ == "__main__":
    # Configuración
    CSV_PATH = 'jugadores_activos_colombianos.csv'
    NUM_PLAYERS = 1000

    
    print(f"\nIniciando extracción de registros de partidos para los primeros {NUM_PLAYERS} jugadores...")
    success = process_player_links(CSV_PATH, NUM_PLAYERS)
    
    if success:
        print("\nProceso completado con éxito")
    else:
        print("\nEl proceso encontró errores. Revisa los mensajes anteriores.")