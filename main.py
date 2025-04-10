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

def scrape_player_data(driver, player_url):
    """Extrae las tablas específicas para un jugador"""
    try:
        driver.get(player_url)
        time.sleep(3)

        target_tables = {
            "all_competitions": None,
            "national_team": None
        }

        def extract_table():
            try:
                html = driver.page_source
                soup = BeautifulSoup(html, 'html.parser')
                table = soup.find('table', {'id': 'stats_standard'})
                if not table:
                    tables = soup.find_all('table', class_='stats_table')
                    for t in tables:
                        caption = t.find('caption')
                        if caption and "Estadísticas estándar" in caption.text:
                            table = t
                            break
                return process_table(table) if table else None
            except Exception as e:
                print(f"Error al extraer tabla: {e}")
                return None

        # Extraer tabla de todas las competencias
        try:
            all_comp_btn = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//div[@class='filter']//a[contains(., 'Todas las competencias')]")))
            
            if "current" not in all_comp_btn.find_element(By.XPATH, "./..").get_attribute("class"):
                driver.execute_script("arguments[0].click();", all_comp_btn)
                time.sleep(3)
            
            target_tables["all_competitions"] = extract_table()
            if target_tables["all_competitions"] is None:
                return None
        except Exception as e:
            print(f"Error con tabla principal: {e}")
            return None

        # Extraer tabla de selección nacional
        try:
            national_btns = driver.find_elements(By.XPATH, "//div[@class='filter']//a[contains(., 'Selección nacional')]")
            if national_btns:
                national_btn = national_btns[0]
                if "current" not in national_btn.find_element(By.XPATH, "./..").get_attribute("class"):
                    driver.execute_script("arguments[0].click();", national_btn)
                    time.sleep(3)
                target_tables["national_team"] = extract_table()
        except Exception as e:
            print(f"No se encontró tabla nacional: {e}")

        return target_tables

    except Exception as e:
        print(f"Error general: {e}")
        return None

def process_table(table):
    """Procesa una tabla HTML a DataFrame"""
    try:
        headers = []
        data = []
        
        header_row = table.find('thead').find_all('tr')[-1]
        headers = [th.text.strip() for th in header_row.find_all(['th', 'td'])]
        
        for row in table.find('tbody').find_all('tr'):
            row_data = [cell.text.strip() for cell in row.find_all(['th', 'td'])]
            if len(row_data) == len(headers):
                data.append(row_data)
        
        return pd.DataFrame(data, columns=headers) if data else None
    except Exception as e:
        print(f"Error procesando tabla: {e}")
        return None

def save_player_tables(tables_dict, player_name, output_dir='jugadores_data'):
    """Guarda las tablas de un jugador en archivos CSV"""
    if not tables_dict or tables_dict.get("all_competitions") is None:
        return False
    
    # Crear directorio si no existe
    os.makedirs(output_dir, exist_ok=True)
    
    # Limpiar el nombre del jugador para el nombre de archivo
    clean_name = re.sub(r'[^\w\s-]', '', player_name).strip().replace(' ', '_')
    base_name = clean_name.lower()
    
    try:
        # Tabla principal
        main_filename = os.path.join(output_dir, f"{base_name}_estadísticas_estándar_table.csv")
        tables_dict["all_competitions"].to_csv(main_filename, index=False, encoding='utf-8-sig')
        
        # Tabla nacional (si existe)
        if tables_dict.get("national_team") is not None:
            national_filename = os.path.join(output_dir, f"{base_name}_estadísticas_estándar_table_seleccion_nacional.csv")
            tables_dict["national_team"].to_csv(national_filename, index=False, encoding='utf-8-sig')
        
        return True
    except Exception as e:
        print(f"Error guardando archivos: {e}")
        return False

def process_player_links(csv_path, num_players=10):
    """Procesa los primeros N jugadores del CSV"""
    try:
        df = pd.read_csv(csv_path)
        # Usamos las columnas 'nombre' y 'href' según tu CSV
        player_links = df['href'].head(num_players).tolist()
        player_names = df['nombre'].head(num_players).tolist()
        
        driver = setup_driver()
        results = []
        
        for i, (url, name) in enumerate(zip(player_links, player_names), 1):
            full_url = f"https://fbref.com{url}" if not url.startswith('http') else url
            print(f"\n[{i}/{num_players}] Procesando {name}...")
            
            tables = scrape_player_data(driver, full_url)
            if tables:
                success = save_player_tables(tables, name)
                if success:
                    results.append((name, "Éxito"))
                    print(f"✔ Datos guardados para {name}")
                else:
                    results.append((name, "Error al guardar"))
            else:
                results.append((name, "Error al extraer datos"))
                print(f"✖ No se pudieron extraer datos para {name}")
            
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
    
    print(f"\nIniciando extracción para los primeros {NUM_PLAYERS} jugadores...")
    success = process_player_links(CSV_PATH, NUM_PLAYERS)
    
    if success:
        print("\nProceso completado con éxito")
    else:
        print("\nEl proceso encontró errores. Revisa los mensajes anteriores.")