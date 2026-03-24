# Nike Data Platform: End-to-End Price Monitoring System

## 1. Problem Statement & Objective

Nike's global distribution involves managing thousands of SKUs across multiple markets, each with its own currency, pricing strategy, and seasonal discounts. **The core challenge** for the business was the lack of a centralized, clean, and reliable data source for global price monitoring. 

Prior to this project, the data was fragmented across 46 weekly snapshots, containing:
*   **Data Integrity Issues**: Over 13,000 duplicate records and 210,000+ price inconsistencies where sale prices were higher than original prices.
*   **High Dimensional Cardinality**: Gender segments were recorded in 13 different ways (e.g., `MEN`, `MEN|WOMEN`, `BOYS`, `GIRLS`), making aggregate reporting near impossible.

**The Objective**: To build an automated data platform that ingests, cleans, and transforms raw market snapshots into a validated **Dimensional Model (Star Schema)**, enabling clear visibility into price trends and discount performance worldwide.

## 2. Architecture & Tech Stack

This project implements a **Modern Data Stack** entirely containerized with Docker:

* **Infrastructure (IaC):** [Terraform](https://www.terraform.io/) for GCP (GCS Bucket & BigQuery Dataset) + Docker for local orchestration.
* **Workflow Orchestration:** [Apache Airflow](https://airflow.apache.org/) (Automated ingestion from Local -> GCS -> BigQuery).
* **Data Warehouse:** [Google BigQuery](https://cloud.google.com/bigquery) (Production target) and PostgreSQL (Local dev).
* **Transformation Layer:** [dbt (BigQuery)](https://www.getdbt.com/) with 14+ automated DQ tests and Star Schema design.
* **Graphical Discovery:** dbt Docs (built-in lineage graph for data traceability).

## 3. Project Structure

* `0_data/`: Raw CSV files. Download the source dataset from [Kaggle: Nike Sales Uncleaned Dataset](https://www.kaggle.com/datasets/nayakganesh007/nike-sales-uncleaned-dataset).
* `1_infrastructure/`: Docker (DB, Airflow, Jupyter), DAGs and environment config.
* `2_EDA/`: Exploratory Data Analysis & DBT Readiness notebooks.
* `3_dbt_project/`: dbt models, tests, and snapshots.

## 4. How to Run

### 1. Pre-requisites (Cloud/GCP Target Only)

If you plan to run the pipeline with the **GCP Cloud target**:

1. Place your GCP Service Account JSON key in the `1_infrastructure/` folder.
2. Rename the file to: **`gcp_service_account.json`**.
3. Set your GCP project details in the `.env file` (see `.env.example`).

### 2. Start Infrastructure

Go to the `1_infrastructure` folder and run:

```bash
docker compose up -d
```

### 2. Ingest & Transform (Automated)

* Access Airflow at `http://localhost:8080` (`airflow/airflow`).
* Trigger the DAG **`ingest_nike_csvs_v1`**.
* This will automatically load the CSVs AND run the dbt transformations.

### 3. Graphical Lineage (dbt-docs)

* Generate the docs metadata: `docker compose run --rm dbt docs generate`
* Start the docs server: `docker compose up -d dbt_docs`
* Access the UI at: **`http://localhost:8082`** (Click the "Lineage Graph" icon).

### 4. Analytical Dashboard

* Access the Streamlit dashboard at: **`http://localhost:8501`**
* The dashboard implements 5 high-impact analytical visualizations:
    1.  **Categorical Analysis**: Average Price by Category and Gender.
    2.  **Temporal Trends**: Historical price evolution (Area Chart).
    3.  **Statistical Distribution**: Box Plots showing price spread and outliers.
    4.  **Product Ranking**: Top 10 most premium SKUs by price.
    5.  **Global Comparison**: Market-by-market price benchmarks.
* **Automated Insights**: 5 key business findings generated directly from the live data.

## 5. Data Quality & Testing

This project prioritizes data reliability. We have implemented **14+ automated data tests** including:

* **Uniqueness**: Ensuring `nike_id` and `fact_key` are truly unique.
* **Referential Integrity**: Every sale in `fct_daily_prices` must map to a valid `dim_product`.
* **Accepted Values**: Gender segments are strictly validated against (MEN, WOMEN, UNISEX, KIDS, OTHER).
* **Price Logic**: Validating that no effective price is null or negative.

## 6. Data Warehouse Optimization

To secure a high-performance analytical layer, we implemented several optimization techniques at the DWH level:

### Materialization Strategy
*   **Staging Layer**: Kept as `views` to ensure data freshness and minimize storage for intermediate cleaning steps.
*   **Marts Layer**: Materialized as `tables`. This is a critical optimization for business users, as it pre-computes the joins and complex logic (`dim_product`, `fct_daily_prices`).

### Indexing & Search Performance
In our local Postgres DWH, we implemented **B-tree Indexing** to optimize the Star Schema:
*   **Fact Table**: `fct_daily_prices` is indexed by `date_day`, `product_key`, and `geography_key` to accelerate time-series analysis and joins.
*   **Dimensions**: `dim_product` and `dim_geography` use **Unique Indexes** on their surrogate keys to ensure sub-millisecond lookups.
* **Partitioning by Day** (`date_day`): This allows BigQuery to scan only the relevant shards for temporal price monitoring queries, reducing slot usage and cost.
* **Clustering by SKU & Geography** (`product_key`, `geography_key`): This physically co-locates rows by product and region, accelerating joins between the fact table and dimensions.
