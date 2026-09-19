# Walmart Data Platform

<p align="center">
  <strong>CDC ingestion, transformation, testing, and history tracking with Airflow, Databricks, and dbt.</strong>
</p>

<p align="center">
  <a href="https://airflow.apache.org/"><img src="https://img.shields.io/badge/Apache%20Airflow-3.3.1-017CEE?logo=apache-airflow&logoColor=white" alt="Apache Airflow 3.3.1"></a>
  <a href="https://www.getdbt.com/"><img src="https://img.shields.io/badge/dbt-1.11.11-FF694A?logo=dbt&logoColor=white" alt="dbt 1.11.11"></a>
  <a href="https://www.databricks.com/"><img src="https://img.shields.io/badge/Databricks-SQL%20Warehouse-FF3621?logo=databricks&logoColor=white" alt="Databricks SQL Warehouse"></a>
  <a href="https://www.docker.com/"><img src="https://img.shields.io/badge/Docker-ready-2496ED?logo=docker&logoColor=white" alt="Docker ready"></a>
</p>

This repository contains a local, Docker-based data platform for Walmart order data. Apache Airflow orchestrates the pipeline, Databricks performs CDC ingestion and hosts the analytical warehouse, and dbt builds tested silver and gold models.

## Architecture

```mermaid
flowchart LR
	A[Operational Walmart data] --> B[Databricks CDC job]
	B --> C[(Databricks catalog<br/>walmart)]

	subgraph O[Airflow orchestration]
		D[orchestrate DAG]
		D1[Freshness check]
		D2[dbt silver_t]
		D3[dbt tests]
		D4[dbt silver_b]
		D5[dbt gold + snapshots]
		D --> D1 --> D2 --> D3 --> D4 --> D5
	end

	B -. triggers and monitors .-> D
	C --> D1
	D2 --> E[(Silver schemas)]
	D4 --> F[(Business OBT)]
	D5 --> G[(Gold dimensions and facts)]

	H[PostgreSQL<br/>Airflow metadata] --- O
	I[Redis<br/>Celery broker] --- O
```

### Pipeline sequence

```mermaid
flowchart TD
	A([Scheduled: 11:00 daily<br/>Asia/Kolkata])
	B[ingest_cdc<br/>Trigger Databricks job and wait]
	C[clear_target<br/>Remove stale dbt target and logs]
	D[source_freshness<br/>Validate source freshness]
	E[silver_technical<br/>dbt run --select silver_t]
	F[silver_technical_test<br/>dbt test --select silver_t]
	G[silver_business<br/>dbt run --select silver_b]
	H[silver_business_test<br/>dbt test --select silver_b]
	I[gold_ephemeral<br/>Build gold/ephemeral]
	J[gold_dimension<br/>dbt snapshot]
	K[gold_fact<br/>Validate gold/fact]
	L([Pipeline complete])

	A --> B --> C --> D --> E --> F --> G --> H --> I --> J --> K --> L
```

The DAG is configured with `catchup=False` and runs once per day. A failed task stops downstream work, so quality checks are part of the delivery path rather than a separate afterthought.

## Data layers

```mermaid
flowchart LR
	S[Databricks CDC sources]
	ST[Silver technical<br/>customers_t<br/>employees_t<br/>orders_t<br/>order_items_t<br/>products_t<br/>stores_t]
	SB[Silver business<br/>obt_b]
	GE[Gold ephemeral<br/>eph_customers<br/>eph_employees<br/>eph_orders<br/>eph_products<br/>eph_stores]
	GD[Gold history<br/>dim_customers snapshot]
	GF[Gold fact<br/>fact_orders]

	S --> ST --> SB --> GE
	GE --> GD
	GE --> GF
```

| Layer | Purpose | Materialization |
| --- | --- | --- |
| `silver_t` | Technical cleanup and CDC-shaped entities | Tables in the `silver_t` schema |
| `silver_b` | Business-level order-wide table (`obt_b`) | Tables in the `silver_b` schema |
| `gold/ephemeral` | Reusable intermediate dimension logic | Ephemeral models |
| `gold` snapshots | Historical dimension records | Snapshot tables in the `gold` schema |
| `gold/fact` | Order-level analytical facts | Tables in the `gold` schema |

## Repository map

```text
.
├── README.md
├── pyproject.toml                         # Python package and dependency metadata
├── src/walmart_dbt/                       # Python package entry point
├── walmart_project/                       # Standalone dbt project
│   ├── models/source/
│   │   ├── silver_t/                     # Technical silver models
│   │   ├── silver_b/                     # Business silver models
│   │   └── gold/                         # Ephemeral and fact models
│   ├── snapshots/                        # Slowly changing dimension snapshots
│   ├── tests/                             # Singular dbt tests
│   ├── macros/                            # Project macros
│   ├── profiles.yml                      # Databricks connection template
│   └── dbt_project.yml
└── walmart_airflow_dbt_project/          # Dockerized Airflow runtime
	├── dags/orchestrate.py               # Pipeline definition
	├── docker-compose.yaml               # Airflow, Redis, and PostgreSQL
	├── Dockerfile
	├── requirements.txt
	└── walmart_project/                   # Mounted dbt project used by Airflow
```

The top-level `walmart_project` and the copy mounted into Airflow should be kept in sync when dbt code changes.

## Quick start

### Prerequisites

- Docker Desktop with at least 4 GB of memory and 2 CPUs allocated
- A Databricks workspace, SQL warehouse, and CDC ingestion job
- Git

### 1. Configure the runtime

From PowerShell:

```powershell
cd .\walmart_airflow_dbt_project
Copy-Item .env.example .env -ErrorAction SilentlyContinue
```

Create `.env` if `.env.example` is not present, then provide the values used by the compose file and dbt profile:

```dotenv
AIRFLOW_UID=50000
FERNET_KEY=<generate-a-fernet-key>
DATABRICKS_HOST=https://<workspace-host>
DATABRICKS_TOKEN=<store-locally-and-never-commit>
```

The current DAG also contains placeholder Databricks connection values in `dags/orchestrate.py`. Replace that connection with environment-backed configuration or an Airflow connection before running against a real workspace. Do not commit tokens, passwords, or keys.

### 2. Build and start Airflow

```powershell
docker compose build
docker compose up airflow-init
docker compose up -d
```

Open the Airflow UI at [http://localhost:8080](http://localhost:8080). The default local credentials are `airflow` / `airflow` unless changed in `.env`.

To follow service logs:

```powershell
docker compose logs -f airflow-scheduler
```

To stop the stack:

```powershell
docker compose down
```

Add `-v` to `docker compose down` only when you intentionally want to remove the local Airflow metadata volume.

## Running dbt locally

The dbt project targets Databricks through `profiles.yml` and reads `DATABRICKS_HOST` and `DATABRICKS_TOKEN` from the environment.

```powershell
cd .\walmart_project
dbt deps
dbt debug
dbt source freshness
dbt run
dbt test
```

Useful focused commands:

```powershell
dbt run --select silver_t
dbt test --select silver_t
dbt run --select silver_b
dbt test --select silver_b
dbt run --select gold/ephemeral
dbt snapshot
dbt test --select gold/fact
```

## Quality and history

- Source freshness runs before transformations.
- Silver technical and business layers have dedicated dbt test stages.
- `tests/test_obt.sql` warns when required order, product, employee, store, order-item, or customer keys are missing from `obt_b`.
- Customer history is captured by the `dim_customers` timestamp snapshot using `customer_id` as the unique key and `customer_updated_timestamp` as the change marker.

## Operations notes

- This compose setup is intended for local development, not production deployment.
- Airflow uses `CeleryExecutor`, PostgreSQL for metadata, and Redis as the Celery broker.
- Airflow starts with DAGs paused. Unpause `orchestrate` in the UI after configuration is complete.
- The DAG currently has a start date of September 21, 2026 and a daily schedule of `0 11 * * *` in `Asia/Kolkata`.
- `target/` and dbt `logs/` are cleared at the beginning of each scheduled run by design.

## Useful references

- [Apache Airflow Docker deployment](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- [dbt documentation](https://docs.getdbt.com/docs/introduction)
- [Databricks SQL connector and dbt](https://docs.getdbt.com/docs/available-adapters/databricks)
