# Proyecto Final ETL - Star Wars API

## Descripción
Este proyecto implementa un proceso ETL que consume datos de la [API pública de Star Wars](https://swapi.dev/), los transforma y los carga en una base de datos MySQL alojada en AWS RDS.

## Requisitos
- Python 3.8+
- MySQL Server (AWS RDS)
- DBeaver (opcional para visualización)
- Entorno virtual (recomendado)

## Instalación

1. Clona el repositorio:
   ```bash
   git clone https://github.com/ashrahx/etl-starwars.git
   cd etl-starwars
   ```

2. Crea y activa un entorno virtual:
   ```bash
   python -m venv venv
   
   source venv/Scripts/activate
   ```

3. Instala las dependencias:
   ```bash
   pip install -r requirements.txt
   ```

4. Configura las credenciales:
   - Edita `config/db_config.yaml` con los datos de tu base de datos.
   - Define el número de tu equipo en `config/config.yaml`.

## Ejecución

- Para ejecutar el proceso manualmente:
  ```bash
  python app.py
  ```

- Para programar ejecución diaria en Windows:
  Ejecuta el archivo por lotes:
  ```bash
  scheduler.bat
  ```

## Estructura del Proyecto

```
.
├── config/
│   ├── config.yaml
│   └── db_config.yaml
├── src/
│   ├── database.py
│   ├── etl_process.py
│   └── logging.py
├── app.py
├── requirements.txt
├── scheduler.bat
└── README.md
```

## Estructura de la Base de Datos

- **DB_ETL_EQUIPO{num}_starwars**
  - `stg_people`
  - `stg_planets`
  - `logs_api_request`

## Licencia

Este proyecto es solo para fines académicos.
