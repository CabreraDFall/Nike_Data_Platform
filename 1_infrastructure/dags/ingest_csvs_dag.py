from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import psycopg2

# Configuración
DATA_FOLDER = "/opt/airflow/0_data"
NIKE_CONN_STRING = "postgresql://admin:adminpassword@nike_postgres/nike_dw"

def get_csv_files():
    return [f for f in os.listdir(DATA_FOLDER) if f.endswith(".csv")]

def ingest_csv_to_postgres(file_name):
    full_path = os.path.join(DATA_FOLDER, file_name)
    table_name = file_name.replace(".csv", "").lower()
    
    conn = psycopg2.connect(NIKE_CONN_STRING)
    cur = conn.cursor()
    
    # Crear esquema y tabla
    cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    cur.execute(f'DROP TABLE IF EXISTS raw."{table_name}";')
        
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
