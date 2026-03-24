from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
import psycopg2
import requests
import csv

# Configuración
DATA_FOLDER = "/opt/airflow/0_data"
NIKE_CONN_STRING = "postgresql://admin:adminpassword@nike_postgres/nike_dw"
DBT_FOLDER = "/opt/airflow/dbt"

def fetch_live_exchange_rates():
    """Obtiene los tipos de cambio actuales desde ExchangeRate-API y los guarda como semilla en dbt."""
    url = "https://api.exchangerate-api.com/v4/latest/USD"
    response = requests.get(url)
    data = response.json()
    
    rates = data.get("rates", {})
    seed_path = os.path.join(DBT_FOLDER, "seeds", "currency_exchange_rates.csv")
    
    os.makedirs(os.path.dirname(seed_path), exist_ok=True)
    
    with open(seed_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["currency_code", "to_usd_rate"])
        for currency, rate in rates.items():
            # La API devuelve el valor de 1 USD en la moneda local. 
            # Para convertir local a USD, multiplicamos por (1 / rate).
            to_usd_rate = 1.0 / rate if rate > 0 else 0
            writer.writerow([currency, to_usd_rate])
            
    print(f"Tipos de cambio actualizados en {seed_path}")

def get_csv_files():
    return [f for f in os.listdir(DATA_FOLDER) if f.endswith(".csv")]

def ingest_csv_to_postgres(file_name):
    full_path = os.path.join(DATA_FOLDER, file_name)
    table_name = file_name.replace(".csv", "").lower()
    
    conn = psycopg2.connect(NIKE_CONN_STRING)
    cur = conn.cursor()
    
    # Crear esquema y tabla
    cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    cur.execute(f'DROP TABLE IF EXISTS raw."{table_name}" CASCADE;')
        
    # Leemos la primera línea para los nombres de las columnas
    with open(full_path, 'r', encoding='utf-8') as f:
        header = f.readline().strip().split(',')
        columns = ", ".join([f'"{col}" TEXT' for col in header])
        cur.execute(f'CREATE TABLE raw."{table_name}" ({columns});')
    
    # Carga masiva
    with open(full_path, 'r', encoding='utf-8') as f:
        next(f) # Saltamos cabecera
        cur.copy_expert(f'COPY raw."{table_name}" FROM STDIN WITH CSV', f)
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"Cargado: {file_name}")

def bulk_ingestion_logic():
    files = get_csv_files()
    for f in files:
        ingest_csv_to_postgres(f)

with DAG(
    dag_id='ingest_nike_csvs_v1',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['nike', 'ingestion'],
) as dag:

    # En Airflow 2, lo más sencillo es un PythonOperator que lo haga todo en bucle 
    # o generar tareas dinámicas. Para 46 archivos, un bucle es muy seguro.
    
    ingest_task = PythonOperator(
        task_id='bulk_ingest_csvs',
        python_callable=bulk_ingestion_logic
    )

    fetch_rates_task = PythonOperator(
        task_id='fetch_live_rates',
        python_callable=fetch_live_exchange_rates
    )

    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command='export DBT_TARGET_PATH=/tmp/dbt_target && cd /opt/airflow/dbt && dbt --no-partial-parse seed'
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='export DBT_TARGET_PATH=/tmp/dbt_target && cd /opt/airflow/dbt && dbt --no-partial-parse run'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='export DBT_TARGET_PATH=/tmp/dbt_target && cd /opt/airflow/dbt && dbt --no-partial-parse test'
    )

    fetch_rates_task >> ingest_task >> dbt_seed >> dbt_run >> dbt_test
