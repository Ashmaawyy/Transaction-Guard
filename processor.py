import sys
import json
from pyspark.sql.functions import col, from_json, current_timestamp, lit, udf, when, concat
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, BooleanType, StringType
from pyspark.sql.streaming import GroupState, GroupStateTimeout
from pyspark.sql import Row
from typing import Iterator, Tuple, Dict, Any

# Import utility modules
from utils.spark_session import create_spark_session, KAFKA_BROKER, SCHEMA_REGISTRY_URL
from utils.helpers import check_impossible_speed, MAX_BELIEVABLE_SPEED_KMH

# --- Configuration ---
KAFKA_TOPIC = "financial_transactions"
REDIS_HOST = "localhost"
REDIS_PORT = "6379"
CHECKPOINT_LOCATION = "./spark_checkpoints/transaction_guard"
BATCH_INTERVAL_SECONDS = 5 # Process micro-batches every 5 seconds
STATE_TIMEOUT_MINUTES = 360 # 6 hours of state retention for a user

# --- Schemas ---

# 1. Input Schema (from Kafka Value)
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("user_id", LongType(), False),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("timestamp_ms", LongType(), False),
    StructField("merchant_id", StringType(), False),
    StructField("latitude", DoubleType(), False),
    StructField("longitude", DoubleType(), False),
])

# 2. State Schema (What Spark remembers for each user)
# We use a simple dictionary structure for GroupState, 
# but defining the keys helps in understanding the state's structure.
STATE_SCHEMA_KEYS = {
    "last_lat": None,       # float
    "last_lon": None,       # float
    "last_ts_ms": None,     # int
    "recent_txn_count": 0   # int
}

# --- Core Business Logic: Stateful Fraud Detection ---

def apply_fraud_rules(user_id: LongType, transactions: Iterator[Row], state: GroupState) -> Iterator[Row]:
    """
    Applies stateful fraud detection rules using mapGroupsWithState.
    This function processes transactions sequentially for a single user,
    updates the user's state (last location/time), and flags fraud.
    """
    
    # 1. Retrieve Current State
    # Spark state is stored as bytes, we typically handle serialization/deserialization.
    # In PySpark's GroupState, state.get() returns the last object updated via state.update().
    current_state = state.get
    if current_state is None:
        # Initialize state for a new user
        current_state = {
            "last_lat": None,
            "last_lon": None,
            "last_ts_ms": None,
            "recent_txn_count": 0
        }
    
    output_records = []
    
    for txn in transactions:
        # Extract current transaction data
        current_lat = txn.latitude
        current_lon = txn.longitude
        current_ts_ms = txn.timestamp_ms
        current_amount = txn.amount
        
        # Initialize fraud flags
        is_fraud = False
        fraud_reason = "VALID"
        
        # --- Rule 1: Impossible Speed Check (Stateful) ---
        last_lat = current_state.get("last_lat")
        last_lon = current_state.get("last_lon")
        last_ts_ms = current_state.get("last_ts_ms")
        
        if last_lat is not None and last_lon is not None:
            time_diff_ms = current_ts_ms - last_ts_ms
            
            is_impossible_speed, speed_kmh = check_impossible_speed(
                last_lat, last_lon, 
                current_lat, current_lon, 
                time_diff_ms, 
                max_speed_kmh=MAX_BELIEVABLE_SPEED_KMH
            )
            
            if is_impossible_speed:
                is_fraud = True
                fraud_reason = f"IMPOSSIBLE_SPEED_({speed_kmh:.2f}km/h)"
        
        # --- Rule 2: High Amount Threshold (Stateless) ---
        if current_amount > 2000.00 and not is_fraud:
            is_fraud = True
            fraud_reason = "HIGH_AMOUNT_THRESHOLD"
            
        # --- Update State for the next transaction ---
        new_state = {
            "last_lat": current_lat,
            "last_lon": current_lon,
            "last_ts_ms": current_ts_ms,
            "recent_txn_count": current_state["recent_txn_count"] + 1
        }
        state.update(new_state)
        
        # Set a timeout to clear the state if the user is inactive
        state.setTimeoutDuration(GroupStateTimeout.ProcessingTimeTimeout)
        
        # 4. Prepare Enriched Output Record
        output_records.append(Row(
            transaction_id=txn.transaction_id,
            user_id=txn.user_id,
            amount=current_amount,
            timestamp_ms=current_ts_ms,
            latitude=current_lat,
            longitude=current_lon,
            is_fraudulent=is_fraud,
            fraud_reason=fraud_reason,
            processing_time_ms=int(current_timestamp().cast("long") * 1000) # Spark processing timestamp
        ))
        
    return iter(output_records)

# --- Main Spark Stream Execution ---

def run_streaming_job():
    """
    Initializes Spark, reads from Kafka, applies stateful logic,
    and writes results to Redis and the data lake.
    """
    try:
        spark = create_spark_session(app_name="TransactionGuardProcessor")

        # 1. Read Stream from Kafka
        df_kafka = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "latest")
            .option("maxOffsetsPerTrigger", 50000) # Control throughput
            .load()
        )

        # 2. Extract and Transform (Cleanse and Deserialize)
        # The 'value' column is the raw transaction JSON string
        df_transactions = df_kafka.select(
            from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("txn"),
            col("timestamp").alias("kafka_ingest_time")
        ).select("txn.*", "kafka_ingest_time")

        # 3. Apply Stateful Processing
        # Group by user_id to ensure transactions are processed sequentially per user
        df_enriched = (
            df_transactions
            .groupBy(col("user_id"))
            .applyInPandasWithState(
                func=apply_fraud_rules,
                outputStructType=df_transactions.schema # We use the same schema initially, then update it below
            )
            .withColumnRenamed("col", "enriched_data") # PySpark uses 'col' as default
            .select(
                col("enriched_data.*")
            )
            # Add final columns for better output structure
            .withColumn("timestamp", (col("timestamp_ms") / 1000).cast("timestamp"))
            .withColumn("processing_time", (col("processing_time_ms") / 1000).cast("timestamp"))
        )
        
        # 4. Write Streams (Sinks)

        # Sink 1: Write Fraud Alerts to Redis (Low Latency Serving Layer)
        # We only write the fraudulent transactions to Redis for immediate alerting.
        query_redis = (
            df_enriched
            .filter(col("is_fraudulent") == True)
            .select(
                # Use transaction_id as the Key, and a JSON object as the Value
                concat(lit("fraud:"), col("transaction_id")).alias("key"),
                col("user_id").cast("string").alias("user_id_str"),
                col("fraud_reason"),
                col("timestamp").cast("string"),
                col("processing_time").cast("string")
            )
            .writeStream
            .outputMode("update") # Only write new/updated fraud alerts
            .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/redis_sink")
            .format("console") # Placeholder: Use 'redis' format with custom connector in production
            .start()
        )
        
        print("--- Started Fraud Alert Stream to Console (Redis Sink Placeholder) ---")

        # Sink 2: Write ALL Enriched Transactions to Data Lake (Batch Storage)
        # Parquet format is optimal for S3/Data Lake analytics.
        query_data_lake = (
            df_enriched
            .writeStream
            .outputMode("append")
            .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/data_lake_sink")
            .format("console") # Placeholder: Use 'parquet' format with path to S3/HDFS
            .start()
        )
        
        print("--- Started Data Lake Stream to Console (Parquet/S3 Sink Placeholder) ---")

        # Wait for the termination of the streaming queries
        spark.streams.awaitAnyTermination()

    except Exception as e:
        print(f"FATAL ERROR in Spark Stream: {e}", file=sys.stderr)
        spark.stop()
        sys.exit(1)

if __name__ == "__main__":
    run_streaming_job()