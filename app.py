import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.database import create_database_and_tables
from src.etl_process import run_etl

if __name__ == "__main__":
    create_database_and_tables()
    run_etl()