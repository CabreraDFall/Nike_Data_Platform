# Nike Data Platform 🚀

This project is a **Modern Data Stack** implementation to process and analyze Nike's global sales data (46 CSV files).

## 🛠️ Tech Stack

- **Infrastructure:** Docker (Postgres, Airflow, pgAdmin)
- **Orchestration:** [Apache Airflow](https://airflow.apache.org/) (LocalExecutor)
- **Transformation:** [dbt (Postgres)](https://www.getdbt.com/)
- **Methodology:** Kimball (Dimensional Modeling)
- **Analysis:** PySpark & Jupyter
- **Visualization:** Power BI

## 📂 Project Structure

- `0_data/`: Raw CSV files (not pushed to GitHub).
- `1_infrastructure/`: 
    - `dags/`: Airflow Python workflows for ingestion.
    - `docker-compose.yml`: Deployment of the full stack.
- `2_dbt_project/`: dbt models, tests, and snapshots.

## 🚀 How to Run

### 1. Start Infrastructure
Go to the `1_infrastructure` folder and run:
```bash
docker compose up -d
```

### 2. Ingest Data
1. Access the Airflow UI at `http://localhost:8080`.
2. Login with `airflow / airflow`.
3. Locate the DAG **`ingest_nike_csvs_v1`** and trigger it using the **Play** button.

### 3. Verify in pgAdmin
1. Access pgAdmin at `http://localhost:8081`.
2. Check the `raw` schema in the `nike_dw` database to see the loaded 46 tables.

### 4. Run Transformations (dbt)
From the `1_infrastructure` folder, run dbt using Docker:
```bash
docker compose run --rm dbt debug
```
