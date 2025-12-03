# 🛡️ The Transaction Guard: Real-Time Fraud Detection Engine

## Project Overview

The **Transaction Guard** is an end-to-end, real-time data engineering project designed to detect suspicious financial activity with sub-second latency. This system shifts from traditional daily batch analysis to immediate **Complex Event Processing (CEP)**, leveraging distributed systems to maintain state and apply sophisticated fraud rules as transactions stream into the system.

This project is built to showcase mastery of **event-driven architecture**, **stateful stream processing**, and **infrastructure-as-code**.

---

## 🏗️ Architecture and Data Flow

The pipeline is organized into three distinct layers: Ingestion, Processing, and Serving. All components are containerized using Docker for ease of deployment.

### Key Architectural Features

* **Event-Driven:** The system reacts immediately to incoming transactions rather than relying on scheduled jobs.
* **Stateful Processing:** The Spark application maintains the history (state) of each user's recent transactions to enable cross-event analysis (e.g., checking location changes).
* **Schema Enforcement:** Kafka is paired with a Schema Registry to enforce a rigid data contract (Avro/Pydantic), guaranteeing data quality upstream.

### Pipeline Diagram

```mermaid
graph TD
    subgraph IngestionLayer[Ingestion Layer]
        A["**Faker Producer**\n(Python/Pydantic)"] -->|Sends Avro Events| B["**Apache Kafka**"]
        SR["**Schema Registry**"] -.->|Validates Schema| B
    end

    subgraph ProcessingLayer[Spark Streaming Processing Layer]
        B -->|Consumes Events| C{"**Spark Structured Streaming**\n(PySpark)"}
        C -->|Reads/Writes History| S["**In-Memory State**\nSliding Windows"]
        S -->|Updates State| C
        C -->|Applies Fraud Rules| D["**Fraud/Valid Flag**"]
    end

    subgraph ServingLayer[Serving & Visualization]
        D -->|Valid Txns| E["**S3 Data Lake**\nBatch Storage"]
        D -->|Fraud Alerts| F["**Redis Cache**\nLow Latency Serving"]
        D -->|Fraud Alerts| G["**Grafana Dashboard**"]
    end
````

-----

## 📁 Project File Architecture

The project is structured to separate configuration, core logic, and utility functions for modularity and maintainability.

```
transaction_guard/
├── .gitignore
├── README.md
├── requirements.txt
├── docker-compose.yaml
├── producer.py
├── processor.py
├── tests/
│   ├── __init__.py
│   ├── test_model.py
│   ├── test_helpers.py
│   └── test_spark_processor.py
└── utils/
    ├── __init__.py
    ├── model.py
    ├── helpers.py
    └── spark_session.py
```

### 📄 Component Descriptions

| File/Folder | Purpose | Details |
| :--- | :--- | :--- |
| `README.md` | **Documentation & Overview** | This file; describes architecture, setup, and core challenges. |
| `requirements.txt` | **Dependencies** | Lists all required Python packages for the producer and processor. |
| `docker-compose.yaml` | **Infrastructure Setup** | Defines and connects the distributed services: **Zookeeper**, **Kafka Broker**, and **Schema Registry**. |
| `producer.py` | **Data Ingestion** | The Python script that uses `Faker` to generate high-volume, mock financial transactions and sends them to Kafka. |
| `processor.py` | **Stream Processing Logic** | The main **PySpark Structured Streaming** job. Handles consumption from Kafka, stateful windowing, and fraud rule application. |
| `tests/` | **Test Suite Directory** | Contains all unit and integration tests for the project. |
| `tests/test_model.py` | **Model Unit Tests** | Verifies that the Pydantic `Transaction` schema correctly **passes** valid data and **rejects** invalid data (e.g., negative amounts, out-of-range coordinates). |
| `tests/test_helpers.py` | **Helper Unit Tests** | Verifies the accuracy of the `haversine_distance_km` and `check_impossible_speed` functions, ensuring the core fraud logic is correct. |
| `tests/test_spark_processor.py` | **Spark Integration Tests** | Verifies the core streaming logic: reading mocked data, applying **stateful logic** (Haversine/speed check), and correctly outputting the fraud flag. This test will use a simulated memory source for fast, isolated testing of the PySpark transformations. |
| `utils/` | **Helper Modules** | Directory containing reusable code to keep the main scripts clean. |
| `utils/model.py` | **Data Contract** | Contains the **Pydantic** `Transaction` class, enforcing schema validation for all incoming data. |
| `utils/helpers.py` | **Business Logic Utilities** | Houses pure, reusable functions like the **Haversine Distance** calculation for geospatial fraud detection. |
| `utils/spark_session.py` | **Initialization** | Helper function to initialize a new **SparkSession**, including necessary Kafka and Avro packages. |

-----

## 🛠️ Tech Stack

| Component | Technology | Purpose in Project |
| :--- | :--- | :--- |
| **Data Contract** | **Python (Pydantic)** | Defines the strict schema for transactions, preventing bad data from entering the stream. |
| **Infrastructure** | **Docker Compose** | Simplifies deployment of the entire distributed system locally. |
| **Message Broker** | **Apache Kafka** | Provides a highly durable, partitioned, and high-throughput buffer for transaction events. |
| **Schema Management** | **Confluent Schema Registry** | Enforces schema validation and manages schema evolution for Avro payloads. |
| **Processing Engine** | **Apache Spark (Structured Streaming)** | Used to perform complex, stateful, and time-windowed transformations on the event stream using PySpark. |
| **Serving Layer** | **Redis** | A low-latency Key-Value store to instantly serve fraud flags to the monitoring dashboard. |

-----

## 📊 Data Model (Pydantic Schema)

The core data contract is defined by the `Transaction` Pydantic model. This ensures every event is structured, strongly typed, and passes initial validation checks (e.g., amount must be positive).

**File:** `utils/model.py`

```python
from pydantic import BaseModel, Field

class Transaction(BaseModel):
    transaction_id: str
    user_id: int
    amount: float
    currency: str = "USD"
    timestamp_ms: int
    merchant_id: str
    latitude: float
    longitude: float

# Additional fields added by the Processor (Spark)
class EnrichedTransaction(Transaction):
    is_fraudulent: bool
    fraud_reason: str
```

-----

## ⚙️ Deployment and Local Setup

This project can be deployed locally using a single `docker-compose up` command, thanks to the containerized environment.

### Prerequisites

1.  **Docker & Docker Compose:** Must be installed and running.
2.  **Python 3.9+:** Required for the producer and the PySpark processor.

### 1\. Launch Infrastructure

Use the provided `docker-compose.yaml` (which includes Zookeeper, Kafka, and Schema Registry) to launch the backbone.

```bash
# Clone the repository
git clone [https://github.com/Ashmaawyy/transaction-guard.git](https://github.com/Ashmaawyy/transaction-guard.git)
cd transaction-guard

# Start all services in detached mode
docker-compose up -d
```

### 2\. Run the Producer

The producer generates realistic, high-volume transaction data and sends it to the Kafka topic named `financial_transactions`.

```bash
# Ensure Python dependencies are installed (pydantic, kafka-python, faker)
pip install -r requirements.txt

# Execute the producer script
python producer.py
```

### 3\. Start the Spark Processor

This step requires connecting the Spark environment to the running Kafka cluster. The Spark job will perform the stateful analysis and write alerts to Redis.

**(To be built in a later step)**

```bash
# Example command structure (using spark-submit inside a container or locally)
spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
    processor.py
```

### 4\. Monitor Alerts

Connect Grafana to the Redis cache to visualize any flagged fraudulent transactions in real-time.

-----

## 🔑 Key Engineering Challenges

This project specifically targets and solves advanced data engineering problems:

1.  **State Management:** Implementing Spark Structured Streaming's **`mapGroupsWithState`** or time-windowing to track historical user activity (e.g., transactions per minute).
2.  **Geospatial Fraud:** Calculating the **Haversine Distance** between the user's current and previous transaction location to detect "impossible speed" travel.
3.  **Backpressure Handling:** Configuring Kafka and Spark to ensure the processor doesn't crash when data ingestion spikes.
4.  **Schema Evolution:** Designing the pipeline to handle changes to the `Transaction` schema gracefully via the Schema Registry.

<!-- end list -->
