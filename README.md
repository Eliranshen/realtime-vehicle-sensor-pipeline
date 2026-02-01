![Spark](https://img.shields.io/badge/Apache_Spark-Structured_Streaming-E25A1C?style=for-the-badge&logo=apachespark)
![Kafka](https://img.shields.io/badge/Apache_Kafka-Streaming_Ingestion-231F20?style=for-the-badge&logo=apachekafka)
![Python](https://img.shields.io/badge/Python-3.x-3776AB?style=for-the-badge&logo=python)
![Airflow](https://img.shields.io/badge/Apache_Airflow-Orchestration-017CEE?style=for-the-badge&logo=apacheairflow)

## 📌 Overview
This project implements a scalable **Streaming ETL Pipeline** designed to ingest, process, and analyze simulated vehicle sensor data in real-time.
The system mimics an IOT environment where cars transmit telemetry data (speed, RPM, gear, location). The pipeline detects anomalies, enriches data with static vehicle dimensions, and aggregates metrics for operational dashboarding.

## 🏗 Architecture & Data Flow
The pipeline follows a modern stream processing architecture:

1.  **Data Ingestion (Kafka):**
    * A Python-based **Data Generator** simulates real-time events from multiple vehicles.
    * Events include: `car_id`, `speed`, `rpm`, `gear`, `location`, `timestamp`.
    * Data is pushed to the Kafka topic: `sensors-sample`.

2.  **Stream Processing (Spark Structured Streaming):**
    * **Enrichment:** The stream is joined in real-time with static reference data (Car Models, Colors) stored in **MINIO LIKE AWS S3** (JSON format).
    * **Transformation:** Deriving new fields (e.g., `expected_gear` calculated based on speed).
    * **Anomaly Detection:** Filtering events based on business logic:
        * High Speed Alerts (`Speed > 120`).
        * Engine Stress (`RPM > 6000`).
        * Gear Mismatch (`Actual Gear != Expected Gear`).

3.  **Aggregation & Analytics:**
    * Calculating sliding window statistics (15-minute windows).
    * Metrics include: Max speed per model, total active cars, color distribution.

4.  **Sinks (Output):**
    * **Alerts:** Critical anomalies are written to a dedicated Kafka topic (`alert-data`) for immediate action.
    * **Analytics:** Aggregated KPIs are outputted to the console (or external database/dashboard).

## 🛠 Tech Stack
* **Ingestion:** Apache Kafka, Zookeeper.
* **Processing:** Apache Spark (Structured Streaming), PySpark.
* **Storage:** MINIO like AWS S3 (Data Lake for dimensional data).
* **Containerization:** Docker (for local environment setup).
* **Language:** Python 3.8+.

## 📂 Project Structure
```text
realtime-vehicle-sensor-pipeline/
│
├── src/
│   ├── dag/
│   │   └── spark_streaming_dag.py   # Airflow DAG for pipeline orchestration
│   │
│   ├── producers/
│   │   └── data_generator.py        # Simulates real-time car sensor data to Kafka
│   │
│   ├── processing/
│   │   ├── data_enrichment.py       # Main ETL: Joins stream with static data
│   │   ├── alert_detection.py       # Logic for detecting anomalies (Speed/RPM)
│   │   └── alert_counter.py         # Aggregates KPIs and statistics
│   │
│   └── setup/
│       ├── car_models.py            # Generates static car model data
│       ├── model_creation.py        # Creates dimension tables (S3/Local)
│       └── cars_generator.py        # Initializes the vehicle fleet reference data
│
├── data/                            # Sample data (JSON/CSV)
├── requirements.txt                 # Project dependencies
└── README.md                        # Project documentation
