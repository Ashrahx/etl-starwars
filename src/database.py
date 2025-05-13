import os
import pymysql
import yaml
from pymysql import Error

# Ruta base del archivo actual (src/database.py)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Rutas absolutas a los archivos de configuración
db_config = os.path.join(BASE_DIR, "config", "db_config.yaml")
main_config = os.path.join(BASE_DIR, "config", "config.yaml")

print("Ruta db_config:", db_config)
print("Ruta config:", main_config)

def get_db_connection(database=None):
   
    with open(db_config) as file:
        config = yaml.safe_load(file)['mysql']
    
    try:
        connection = pymysql.connect(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            password=config['password'],
            database=database,
            charset=config['charset'],
            cursorclass=pymysql.cursors.DictCursor
        )
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        raise

def create_database_and_tables():
    with open(main_config) as file:
        config = yaml.safe_load(file)
    
    db_name = f"DB_ETL_EQUIPO{config['team']['number']}_{config['team']['name']}"
    
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            # Crear base de datos
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            cursor.execute(f"USE {db_name}")
            
            # Tabla para Personajes
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {config['tables']['people']} (
                    uid INT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    height VARCHAR(20),
                    mass VARCHAR(20),
                    hair_color VARCHAR(50),
                    skin_color VARCHAR(50),
                    eye_color VARCHAR(50),
                    birth_year VARCHAR(20),
                    gender VARCHAR(20),
                    homeworld VARCHAR(100),
                    url VARCHAR(255),
                    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Tabla para Planetas
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {config['tables']['planets']} (
                    uid INT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    diameter VARCHAR(20),
                    rotation_period VARCHAR(20),
                    orbital_period VARCHAR(20),
                    gravity VARCHAR(50),
                    population VARCHAR(50),
                    climate VARCHAR(100),
                    terrain VARCHAR(100),
                    surface_water VARCHAR(20),
                    url VARCHAR(255),
                    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Tabla de logs
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {config['tables']['logs']} (
                    log_id INT AUTO_INCREMENT PRIMARY KEY,
                    endpoint VARCHAR(255) NOT NULL,
                    status INT NOT NULL,
                    request_timestamp DATETIME NOT NULL,
                    error_message TEXT,
                    records_processed INT
                )
            """)
            
        connection.commit()
        print(f"Base de datos '{db_name}' y tablas creadas exitosamente")
    finally:
        connection.close()