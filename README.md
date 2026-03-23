# Nike Data Platform 🚀

This project is a **Modern Data Stack** implementation to process and analyze Nike's global sales data (46 CSV files).

## 🛠️ Tech Stack

- **Infrastructure:** Docker (Postgres, Kestra, pgAdmin)
- **Orchestration:** [Kestra](https://kestra.io/)
- **Transformation:** [dbt (Postgres)](https://www.getdbt.com/)
- **Methodology:** Kimball (Dimensional Modeling)
- **Analysis:** PySpark & Jupyter
- **Visualization:** Power BI

## 📂 Project Structure

- `0_data/`: Raw CSV files (not pushed to GitHub).
- `1_infrastructure/`: Docker Compose and infrastructure configuration.
- `2_dbt_project/`: dbt models, tests, and snapshots.

## 🚀 How to Run

### 1. Start Infrastructure
Go to the `1_infrastructure` folder and run:
```bash
docker-compose up -d
```

### 2. Run dbt
Go to the `1_infrastructure` folder and use Docker to run dbt:
```bash
docker-compose run --rm dbt debug
```

---
