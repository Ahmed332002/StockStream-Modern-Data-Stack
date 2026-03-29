# 📈 End-to-End Real-Time Data Platform: Global Stock Market Analytics

An enterprise-grade **Modern Data Stack** platform designed to transform high-frequency, fragmented stock market data into a unified analytics foundation. The system implements a **Hybrid Architecture** (Streaming + Batch) to support real-time operational monitoring and deep historical analysis.

## 📋 Table of Contents
- [Business Context](#-business-context)
- [Architecture Overview](#-architecture-overview)
- [Problem Statement](#-problem-statement)
- [Solution Design](#-solution-design)
- [Project Structure](#-project-structure)
- [Technology Stack](#-technology-stack)
- [Data Pipeline](#-data-pipeline)
- [Key Features](#-key-features)
- [Deliverables](#-deliverables)
- [Installation--setup](#-installation--setup)

---

## 🎯 Business Context
The financial data landscape is characterized by extreme volatility and high volume. This platform was built to solve critical data challenges for financial analysts:
* **Global Coverage**: Tracking ticker symbols across multiple exchanges.
* **Real-time Volatility**: Prices change in milliseconds, requiring sub-second ingestion.
* **Complex Metadata**: Managing frequent changes in company info using SCDs.
* **Data Volume**: Handling millions of price points daily without performance degradation.

---

## 🏗️ Architecture Overview
The platform implements a **Hybrid Lambda-style Architecture** for maximum reliability:

**Data Sources (Market APIs / WebSockets)**
    ↓

```text
┌─────────────────────────────────┬──────────────────────────────────┐
│      STREAMING LAYER (Hot Path) │      BATCH LAYER (Cold Path)     │
├─────────────────────────────────┼──────────────────────────────────┤
│ • Kafka Producer (Python)       │ • Airflow Orchestration          │
│ • Avro Serialization            │ • Snowflake Stage (Internal)     │
│ • Schema Registry (Governance)  │ • dbt Transformations            │
│ • MinIO Data Lake (S3-Compat)   │ • Medallion (Bronze/Silver/Gold) │
└─────────────────────────────────┴──────────────────────────────────┘
````

```
↓                                   ↓
```

**SERVING LAYER: Snowflake Data Warehouse**
↓

```text
┌─────────────────────────────────────────┐
│            CONSUMER LAYER               │
├──────────────────┬──────────────────────┤
│    Power BI      │    Snowflake SQL     │
│  (Historical BI) │    (Ad-hoc Insights)  │
└──────────────────┴──────────────────────┘
```

-----

## 🔍 Problem Statement

| Challenge | Impact | Solution |
| :--- | :--- | :--- |
| **Data Latency** | 24-hour lag prevents intra-day trading insights | Kafka Streaming + Micro-batching (\<1 min latency) |
| **Schema Drift** | Broken pipelines when API formats change | **Confluent Schema Registry (Avro)** enforcement |
| **Data Silos** | Prices and Metadata stored in incompatible formats | Unified **Medallion Architecture** in Snowflake |
| **History Loss** | Overwriting company info hides historical context | **dbt SCD Type 2 Snapshots** for Dimensions |
| **Cost Control** | Full-refreshing large tables is expensive | **Incremental Materialization** for delta updates |

-----

## 💡 Solution Design

### 1\. Streaming Layer (Hot Path)

  * **Purpose**: Real-time event capture with high throughput and schema governance.
  * **Flow**:
      * **Data Ingestion (`producer.py`)**: Python-based producers fetching real-time market ticks.
      * **Metadata Management (`fetch_metadata.py`)**: Automated scripts to fetch and sync company reference data (`companies_metadata.json`).
      * **Serialization**: **Avro** format used to minimize payload size and enforce schema.
      * **Governance**: **Schema Registry** ensures compatibility between producers and consumers.
      * **Sinking (`consumer.py`)**: Distributed consumers encoding data into Avro and sinking to **MinIO (S3)**.

### 2\. Batch Layer (Cold Path) - Analytics Engineering

  * **Purpose**: Deep historical analysis and business intelligence.
  * **Medallion Strategy**:
      * **Bronze Layer (Staging)**: Raw JSON/Avro landing with metadata schemas (`src_stocks.yml`).
      * **Silver Layer (Intermediate)**: De-duplication, data type casting, and currency normalization.
      * **Gold Layer (Marts)**: Dimensional modeling (**Galaxy Schema**) with Fact (`fct_stock_prices`) and Dims (`dim_companies`, `dim_assets`).
  * **Orchestration**: **Airflow + Cosmos** automate the transition from MinIO to Snowflake (`minio_to_snowflake.py`).

-----

## 📁 Project Structure

```text
FINAL/
├── dags/                         # Airflow Orchestration
│   └── minio_to_snowflake.py      # Main Pipeline DAG
├── dbt_stocks/                   # dbt Project (Analytics Engineering)
│   ├── models/
│   │   ├── marts/                # Gold Layer (Galaxy Schema)
│   │   │   ├── dim_assets.sql
│   │   │   ├── dim_companies.sql
│   │   │   ├── dim_date.sql
│   │   │   └── fct_stock_prices.sql
│   │   └── staging/              # Bronze/Silver Layer
│   │       ├── stg_companies.sql
│   │       └── stg_stock_prices.sql
│   ├── snapshots/                # SCD Type 2 (scd_companies.sql)
│   └── dbt_project.yml           # dbt Configuration
├── docker-compose.override.yml   # Infrastructure (Kafka, Zookeeper, Schema Registry, MinIO, Kafdrop)
├── Dockerfile                  # Astro Runtime 3.1-14 Custom Image
├── consumer.py                   # Kafka to MinIO S3 Sink
├── producer.py                   # Market Data Ingestion (Avro)
└── fetch_metadata.py             # Company Metadata Sync Script
```

-----

## 🛠️ Technology Stack

  * **Ingestion**: Python, Apache Kafka, Confluent Schema Registry (Avro).
  * **Storage**: MinIO (S3-Compatible Object Storage).
  * **Warehouse**: Snowflake (Cloud Data Warehouse).
  * **Transformation**: dbt (Data Build Tool), SQL.
  * **Orchestration**: Apache Airflow (Astro) & Astronomer Cosmos.
  * **DevOps**: Docker & Docker Compose.

-----

## 📊 Data Pipeline

### Batch Pipeline (Daily/Micro-batch)

1.  **Source**: MinIO "Inbox" bucket.
2.  **Ingestion**: Airflow triggers `PUT` & `COPY INTO` to Snowflake internal stages.
3.  **Silver Layer**: dbt cleanses data and handles **Missing Assets** via Dynamic Dummies.
4.  **Gold Layer**: Builds `fct_stock_prices` and `dim_companies` using Galaxy Schema.
5.  **Success Criteria**: 100% Data Integrity, 0% Orphaned Price Records.

### Streaming Pipeline (Real-time)

1.  **Producer**: Captures price change -\> Encodes to Avro -\> Pushes to Kafka.
2.  **Kafka**: Distributed topics with fault-tolerant replication.
3.  **Consumer**: Decodes via Registry -\> Sinks to MinIO Data Lake.
4.  **Latency**: \<100ms Ingestion Latency.

-----

## ✨ Key Features

1.  **SCD Type 2 Snapshots**: Utilizes dbt Snapshots to track historical changes in company metadata.
2.  **Incremental Loading**: Optimized Fact tables to process only new deltas, reducing Snowflake costs.
3.  **Data Quality Framework**: Enforced via `dbt_packages` for unique keys and referential integrity.
4.  **Automated Metadata Sync**: `fetch_metadata.py` ensures the warehouse has the latest company profiles.
5.  **Dynamic Dummies**: Automated handling of non-stock assets (Crypto/ETFs) during transformation.

-----

## 📦 Deliverables

1.  **Snowflake Data Warehouse**: Fully modeled Galaxy Schema (Bronze/Silver/Gold).
2.  **Power BI Dashboard**: Real-time vs. Historical Price Trends.
3.  **Auto-docs**: dbt generated documentation and lineage graphs.
4.  **Containerized Stack**: Seamless deployment via Docker Compose.

-----

## 🚀 Installation & Setup

1.  **Clone the Repository**:

    ```bash
    git clone [https://github.com/AhmedNabil/StockStream-Modern-Data-Stack.git](https://github.com/AhmedNabil/StockStream-Modern-Data-Stack.git)
    ```

2.  **Setup Astro Environment**:
    Ensure you have the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) installed on your machine.

3.  **Configure Environment**:
    Update the `.env` file with your **Snowflake**, **MinIO**, and **Kafka** credentials to ensure seamless connectivity.

4.  **Start the Platform**:

    ```bash
    astro dev start
    ```

*This initializes the Astronomer runtime (v3.1-14), spinning up the Airflow webserver, scheduler, and internal database via Docker.*

#### 🔍 Verify the Stack:

  * **Airflow UI**: [http://localhost:8080](https://www.google.com/search?q=http://localhost:8080)
  * **Kafdrop (Kafka UI)**: [http://localhost:9000](https://www.google.com/search?q=http://localhost:9000)
  * **MinIO Console**: [http://localhost:9001](https://www.google.com/search?q=http://localhost:9001)

<!-- end list -->

```
```