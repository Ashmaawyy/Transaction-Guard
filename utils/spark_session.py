from pyspark.sql import SparkSession
from typing import Optional

# --- Configuration ---
# Required Maven packages for Structured Streaming to connect to Kafka and Redis.
# The versions must match your Spark version (we use 3.5.0, so the Scala version is 2.12).
SPARK_PACKAGES = [
    # Kafka I/O for reading the source stream
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    # Spark Connect (if running remotely)
    "org.apache.spark:spark-connect_2.12:3.5.0",
    # (Optional, but often needed for production sinks like JDBC/Redis)
    # Example for Redis sink (requires a custom connector jar or module)
    # "com.redislabs:spark-redis_2.12:3.0.0" 
]

# Kafka Broker Address - Should match the address in docker-compose.yaml
KAFKA_BROKER = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

def create_spark_session(app_name: str = "TransactionGuardApp") -> SparkSession:
    """
    Creates a pre-configured SparkSession with all necessary external packages
    and configurations for the streaming job.

    Args:
        app_name: The name for the Spark application.

    Returns:
        A configured SparkSession instance.
    """
    
    # 1. Join packages into the required string format
    packages_string = ",".join(SPARK_PACKAGES)
    
    # 2. Build the Spark Session
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")  # Use all available cores locally
        .config("spark.jars.packages", packages_string)
        # Set minimum logging level for cleaner console output
        .config("spark.driver.maxResultSize", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )
    
    # Set the logging level for Spark itself to minimize noise during streaming
    spark.sparkContext.setLogLevel("WARN")

    print(f"--- Spark Session '{app_name}' Initialized ---")
    print(f"Using Packages: {packages_string}")
    
    return spark

# Example usage (for testing/debugging outside the processor.py)
if __name__ == '__main__':
    spark = create_spark_session()
    print(f"Spark Version: {spark.version}")
    spark.stop()