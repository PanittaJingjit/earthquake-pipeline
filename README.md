# 🌍 Earthquake Data Pipeline

An end-to-end Data Engineering pipeline that automatically collects real-time earthquake data from the USGS Earthquake API, processes the data through ETL workflows, and stores the transformed data into PostgreSQL using Apache Airflow and Docker.

---

# 📌 Project Overview

This project demonstrates a complete ETL (Extract, Transform, Load) pipeline for earthquake monitoring data.

The pipeline:

1. Extracts earthquake data from the USGS public API
2. Cleans and validates the raw JSON data
3. Transforms the dataset into analytics-ready features
4. Loads the processed data into PostgreSQL
5. Automates the workflow using Apache Airflow

---

# 🏗️ Architecture

USGS API
   ↓
Ingestion Layer
   ↓
Raw JSON Storage
   ↓
Data Cleaning
   ↓
Feature Engineering / Transformation
   ↓
PostgreSQL Data Warehouse
   ↓
Analytics / SQL Queries

---

# ⚙️ Technologies Used

- Python
- Apache Airflow
- PostgreSQL
- Docker & Docker Compose
- Pandas
- SQLAlchemy
- Requests

---

# 📂 Project Structure
```text
earthquake-pipeline/
│
├── airflow/
│   └── dags/
│       └── earthquake_pipeline_dag.py
│
├── scripts/
│   ├── ingest_earthquake.py
│   ├── clean_earthquake.py
│   ├── transform_earthquake.py
│   └── load_earthquake.py
│
├── data/
│   ├── raw/
│   └── processed/
│
├── logs/
│
├── docker-compose.yml
│
└── README.md
```

---

# 🔄 ETL Workflow

## 1️⃣ Ingestion

- Fetches earthquake data from:
  https://earthquake.usgs.gov/

- Stores raw JSON files inside:

data/raw/

---

## 2️⃣ Cleaning

The cleaning process:

- Removes duplicate earthquake IDs
- Validates coordinates
- Converts data types
- Handles missing values
- Splits location information
- Standardizes timestamps

### Example

Original API field:

6 km SSW of Redlands, CA

Split into:

- location → 6 km SSW of Redlands
- country → CA

---

## 3️⃣ Transformation

Additional engineered features:

- severity classification
- depth category
- magnitude category
- risk score
- time features
- hemisphere classification
- day/night classification

---

## 4️⃣ Loading

The transformed dataset is loaded into:

PostgreSQL Schema: silver

Table:

silver.cleaned_earthquakes

---

# 🗄️ Database Schema

| Column | Description |
|---|---|
| earthquake_id | Unique earthquake ID |
| magnitude | Earthquake magnitude |
| place | Original location text |
| location | Cleaned location |
| country | Country/region |
| event_time | Event timestamp |
| longitude | Longitude |
| latitude | Latitude |
| depth | Earthquake depth |
| severity | Severity level |
| risk_score | Calculated risk score |

---

# 🚀 How to Run the Project

## 1️⃣ Clone Repository

```bash
git clone https://github.com/your-username/earthquake-pipeline.git
cd earthquake-pipeline
