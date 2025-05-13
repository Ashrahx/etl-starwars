from datetime import datetime
from .database import get_db_connection
import yaml
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
main_config = os.path.join(BASE_DIR, "config", "config.yaml")

def log_api_request(endpoint, status, records_processed=0, error_message=None):
    """
    Registra un request a la API en la tabla de logs
    """
    with open(main_config) as file:
        config = yaml.safe_load(file)
    
    db_name = f"DB_ETL_EQUIPO{config['team']['number']}_{config['team']['name']}"
    connection = get_db_connection(db_name)
    
    try:
        with connection.cursor() as cursor:
            query = f"""
                INSERT INTO {config['tables']['logs']} 
                (endpoint, status, request_timestamp, error_message, records_processed)
                VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(query, (
                endpoint,
                status,
                datetime.now(),
                error_message,
                records_processed
            ))
        connection.commit()
    except Exception as e:
        print(f"Error al registrar log: {str(e)}")
    finally:
        connection.close()