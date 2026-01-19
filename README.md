# 📊 Incremental Crypto Data Pipeline

## Overview
This project builds an **end-to-end incremental data pipeline** that fetches cryptocurrency data from the **CoinGecko API**, processes it using **Apache Spark**, stores it in **Delta Lake on MinIO**, loads analytics data into **PostgreSQL**, and visualizes insights using **Apache Superset**.

The pipeline is fully **Dockerized** and follows a **Bronze → Silver → Gold** architecture.

---

## Architecture
CoinGecko API  
→ Apache Spark (Incremental ETL)  
→ Delta Lake (MinIO)  
→ PostgreSQL  
→ Apache Superset

---

## Tech Stack
- Apache Spark (PySpark)
- Delta Lake
- MinIO (S3 compatible)
- PostgreSQL
- Apache Superset
- Docker & Docker Compose

---

## Key Features
- Incremental data loading using metadata tracking
- ACID-compliant Delta tables
- Schema evolution support
- Analytics-ready PostgreSQL layer
- Superset dashboards for visualization

---

## How to Run
```bash
docker-compose up -d
docker exec -it coinapi_app python app/scripts/run_pipeline.py
