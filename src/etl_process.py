import requests
import os
import json
import yaml
from datetime import datetime
from .database import get_db_connection
from .logging import log_api_request

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

db_config = os.path.join(BASE_DIR, "config", "db_config.yaml")
main_config = os.path.join(BASE_DIR, "config", "config.yaml")

def extract_data_from_api(endpoint):
    """
    Extrae datos de la API
    """
    with open(main_config) as file:
        config = yaml.safe_load(file)
    
    base_url = config['api']['base_url']
    url = f"{base_url}/{endpoint}"
    all_results = []
    
    try:
        # Solicitud
        response = requests.get(url, timeout=config['api']['timeout'])
        response.raise_for_status()
        data = response.json()
        
        print(f"Respuesta inicial de {endpoint}: Total records={data.get('total_records', 0)}")
        
        # Procesar la primera página de resultados
        if 'results' in data:
            for item in data['results']:
                detail_url = item.get('url')
                if detail_url:
                    try:
                        # Obtener detalles completos de cada item
                        detail_response = requests.get(detail_url, timeout=config['api']['timeout'])
                        detail_response.raise_for_status()
                        detail_data = detail_response.json()
                        
                        if 'result' in detail_data:
                            all_results.append(detail_data['result'])
                    except requests.exceptions.RequestException as e:
                        print(f"Error obteniendo detalles de {detail_url}: {str(e)}")
                        continue
        
        # Manejar paginación si es necesario
        next_page = data.get('next')
        while next_page:
            try:
                response = requests.get(next_page, timeout=config['api']['timeout'])
                response.raise_for_status()
                page_data = response.json()
                
                if 'results' in page_data:
                    for item in page_data['results']:
                        detail_url = item.get('url')
                        if detail_url:
                            try:
                                detail_response = requests.get(detail_url, timeout=config['api']['timeout'])
                                detail_response.raise_for_status()
                                detail_data = detail_response.json()
                                
                                if 'result' in detail_data:
                                    all_results.append(detail_data['result'])
                            except requests.exceptions.RequestException as e:
                                print(f"Error obteniendo detalles de {detail_url}: {str(e)}")
                                continue
                
                next_page = page_data.get('next')
            except requests.exceptions.RequestException as e:
                print(f"Error obteniendo página {next_page}: {str(e)}")
                break
        
        print(f"Total de {len(all_results)} elementos completos obtenidos para {endpoint}")
        return all_results
    
    except requests.exceptions.RequestException as e:
        log_api_request(
            endpoint=endpoint,
            status=500,
            error_message=str(e)
        )
        raise

def transform_people_data(raw_data):
    """
    Transforma datos de personajes (versión robusta)
    """
    transformed = []
    for person in raw_data:
        try:
            if not person or 'properties' not in person:
                continue
                
            properties = person.get('properties', {})
            
            # Validar campos requeridos
            if not properties.get('name'):
                continue
                
            transformed.append({
                'uid': int(person.get('uid', 0)),
                'name': properties.get('name', 'Desconocido'),
                'height': properties.get('height', 'unknown'),
                'mass': properties.get('mass', 'unknown'),
                'hair_color': properties.get('hair_color', 'unknown'),
                'skin_color': properties.get('skin_color', 'unknown'),
                'eye_color': properties.get('eye_color', 'unknown'),
                'birth_year': properties.get('birth_year', 'unknown'),
                'gender': properties.get('gender', 'unknown'),
                'homeworld': properties.get('homeworld', ''),
                'url': properties.get('url', '')
            })
        except Exception as e:
            print(f"Error transformando personaje: {e}")
            continue
            
    return transformed

def transform_planets_data(raw_data):
    """
    Transforma datos de planetas
    """
    transformed = []
    for planet in raw_data:
        try:
            if not planet or 'properties' not in planet:
                continue
                
            properties = planet.get('properties', {})
            
            if not properties.get('name'):
                continue
                
            transformed.append({
                'uid': int(planet.get('uid', 0)),
                'name': properties.get('name', 'Desconocido'),
                'diameter': properties.get('diameter', '0'),
                'rotation_period': properties.get('rotation_period', '0'),
                'orbital_period': properties.get('orbital_period', '0'),
                'gravity': properties.get('gravity', 'N/A'),
                'population': properties.get('population', '0'),
                'climate': properties.get('climate', 'unknown'),
                'terrain': properties.get('terrain', 'unknown'),
                'surface_water': properties.get('surface_water', '0'),
                'url': properties.get('url', '')
            })
        except Exception as e:
            print(f"Error transformando planeta: {e}")
            continue
            
    return transformed
    
def load_data(data, table_name):
    """
    Carga datos transformados a la base de datos
    """
    with open(main_config) as file:
        config = yaml.safe_load(file)
    
    db_name = f"DB_ETL_EQUIPO{config['team']['number']}_{config['team']['name']}"
    connection = get_db_connection(db_name)
    
    try:
        with connection.cursor() as cursor:
            # Determinar las columnas basadas en el primer registro
            if not data:
                print(f"No hay datos válidos para cargar en {table_name}")
                return
                
            columns = data[0].keys()
            placeholders = ', '.join(['%s'] * len(columns))
            columns_str = ', '.join(columns)
            
            # Crear consulta de inserción
            query = f"""
                INSERT INTO {table_name} ({columns_str})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE
                {', '.join([f"{col}=VALUES({col})" for col in columns if col != 'uid'])}
            """
            
            # Insertar todos los registros
            cursor.executemany(query, [tuple(item.values()) for item in data])
            
        connection.commit()
        print(f"{len(data)} registros cargados en {table_name}")
        
    except Exception as e:
        print(f"Error al cargar datos en {table_name}: {str(e)}")
        raise
    finally:
        connection.close()

def run_etl():
    """
    Ejecuta el proceso ETL
    """
    print("Iniciando proceso ETL para SWAPI...")
    
    try:
        # 1. Extraer datos
        print("\n=== FASE DE EXTRACCIÓN ===")
        print("Extrayendo datos de personajes...")
        people_data = extract_data_from_api("people")
        print(f"Datos crudos de personajes recibidos: {len(people_data)} elementos")
        
        print("\nExtrayendo datos de planetas...")
        planets_data = extract_data_from_api("planets")
        print(f"Datos crudos de planetas recibidos: {len(planets_data)} elementos")
        
        # 2. Transformar datos
        print("\n=== FASE DE TRANSFORMACIÓN ===")
        print("Transformando datos de personajes...")
        transformed_people = transform_people_data(people_data)
        print(f"Personajes válidos después de transformación: {len(transformed_people)}")
        
        print("\nTransformando datos de planetas...")
        transformed_planets = transform_planets_data(planets_data)
        print(f"Planetas válidos después de transformación: {len(transformed_planets)}")
        
        # 3. Cargar datos
        print("\n=== FASE DE CARGA ===")
        with open(main_config) as file:
            config = yaml.safe_load(file)
        
        if transformed_people:
            print("\nCargando personajes...")
            load_data(transformed_people, config['tables']['people'])
        else:
            print("No hay personajes válidos para cargar")
            
        if transformed_planets:
            print("\nCargando planetas...")
            load_data(transformed_planets, config['tables']['planets'])
        else:
            print("No hay planetas válidos para cargar")
        
        print("\nProceso ETL completado!")
        
    except Exception as e:
        print(f"\nERROR en el proceso ETL: {str(e)}")
        log_api_request(
            endpoint="ETL Process",
            status=500,
            error_message=str(e),
            records_processed=(len(transformed_people) if 'transformed_people' in locals() else 0) + 
                           (len(transformed_planets) if 'transformed_planets' in locals() else 0)
        )
        raise