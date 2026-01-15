# NFL Data Engineering Pipeline

## Overview
This is an end to end data pipeline, designed to ingest, transform, and model NFL Game data from the ESPN Hidden API.

---

## Architecture
**Current Stack**
- **PostgreSQL** – Source and analytics database
- **Apache Airflow** – Orchestration and scheduling
- **dbt** – Data transformation, testing, and modeling
- **Docker / Docker Compose** – Local environment and service management

**Planned**
- **Apache Superset** – BI and visualization layer (to be implemented)

---

## Data Flow
1. Raw NFL data is extracted via the EPNS Hidden API and landed in PostgreSQL
2. Airflow orchestrates ingestion and transformation workflows
3. dbt transforms the raw data into cleaned, analytics ready models
4. Modeled tables are designed for downstream BI and reporting
5. (Planned) Superset will sit on top of the analytics layer for dashboards and exploration

---

## Project Structure
```text
.
├── airflow/
│   └── dags/                # Airflow DAGs for orchestration
├── api-request/             # Data ingestion logic
├── dbt/
│   ├── models/              # dbt models (staging, intermediate, marts)
│   ├── tests/               # dbt tests
│   └── macros/              # dbt macros
├── postgres/                # Database initialization / schemas
├── docker/
│   └── Dockerfiles          # Service definitions
├── docker-compose.yaml      # Local orchestration
└── README.md
