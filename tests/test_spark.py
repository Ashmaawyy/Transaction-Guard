import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_json, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType
from utils.helpers import check_impossible_speed # The core fraud logic
from datetime import datetime
import time

# NOTE: This test assumes that utils/spark_session.py provides a fixture
# or a way to get a pre-configured SparkSession.

# ----------------------------------------------------
# 1. SETUP: Define Schema and UDFs used in the processor
# ----------------------------------------------------

# Define the schema of the data coming from Kafka (Value field)
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

# Define a UDF wrapper for the impossible speed check
# We use the actual helper function here for fidelity
@udf(returnType=StructType([
    StructField("is_fraud", BooleanType(), False),
    StructField("speed_kmh", DoubleType(), False)
]))
def check_impossible_speed_udf(
    last_lat, last_lon, current_lat, current_lon, time_diff_ms
):
    """UDF wrapper for the core geospatial fraud check."""
    # Handle initial records where 'last' data might be null/None
    if last_lat is None or last_lon is None:
        return {"is_fraud": False, "speed_kmh": 0.0}
        
    is_fraud, speed = check_impossible_speed(
        last_lat, last_lon, current_lat, current_lon, time_diff_ms
    )
    return {"is_fraud": is_fraud, "speed_kmh": speed}

# ----------------------------------------------------
# 2. TEST FIXTURE (Requires SparkSession from utils/spark_session.py)
# ----------------------------------------------------

# Note: In a real environment, you would use a pytest fixture here 
# that calls a function in utils/spark_session.py to initialize Spark.
# For simplicity, we create a basic session here.

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Creates a basic Spark Session for testing."""
    spark = (
        SparkSession.builder
        .appName("TestTransactionGuard")
        .master("local[*]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


# ----------------------------------------------------
# 3. CORE INTEGRATION TESTS
# ----------------------------------------------------

def test_geospatial_fraud_detection(spark: SparkSession):
    """
    Tests the impossible speed detection logic using simulated transaction pairs.
    Case 1: FRAUD (NYC to London in 30 minutes)
    Case 2: VALID (NYC to London in 10 hours)
    """
    
    # --- Define Test Data ---
    # NYC Coordinates
    nyc_lat, nyc_lon = 40.7128, -74.0060 
    # London Coordinates
    london_lat, london_lon = 51.5074, 0.1278 
    
    # 1. Base transaction (happened 10 hours ago)
    txn_t0_ts = int(time.time() * 1000) - (10 * 60 * 60 * 1000)
    
    # 2. Case 1: FRAUD - High distance in short time (30 minutes)
    txn_fraud_ts = txn_t0_ts + (30 * 60 * 1000) 
    
    # 3. Case 2: VALID - High distance in long time (10 hours)
    txn_valid_ts = txn_t0_ts + (10 * 60 * 60 * 1000) 

    # Mocked data structure for testing the UDF (assuming 'last' and 'current' columns are available)
    # In the real processor, we would use mapGroupsWithState to generate the 'last' columns.
    mock_data = [
        # (User ID, Current Lat, Current Lon, Last Lat, Last Lon, Time Diff MS, Expected Fraud?)
        # 1. VALID: This is the first transaction, so last location is NULL.
        (101, nyc_lat, nyc_lon, None, None, 0), 
        
        # 2. FRAUD: Moves 5500km in 30 minutes.
        (102, london_lat, london_lon, nyc_lat, nyc_lon, txn_fraud_ts - txn_t0_ts), 
        
        # 3. VALID: Moves 5500km in 10 hours.
        (103, london_lat, london_lon, nyc_lat, nyc_lon, txn_valid_ts - txn_t0_ts), 
    ]
    
    # Define the mock schema structure for the test data
    test_schema = StructType([
        StructField("user_id", LongType(), True),
        StructField("current_lat", DoubleType(), True),
        StructField("current_lon", DoubleType(), True),
        StructField("last_lat", DoubleType(), True),
        StructField("last_lon", DoubleType(), True),
        StructField("time_diff_ms", LongType(), True),
    ])

    df = spark.createDataFrame(mock_data, schema=test_schema)
    
    # --- Apply Transformation (Same logic as processor.py) ---
    
    # Apply the UDF to create a struct containing the fraud flag and speed
    df_result = df.withColumn(
        "fraud_check",
        check_impossible_speed_udf(
            col("last_lat"), 
            col("last_lon"), 
            col("current_lat"), 
            col("current_lon"), 
            col("time_diff_ms")
        )
    ).withColumn(
        "is_fraudulent", col("fraud_check.is_fraud")
    ).withColumn(
        "speed_kmh", col("fraud_check.speed_kmh")
    ).drop("fraud_check")
    
    # Collect results for assertion
    results = df_result.collect()

    # --- Assertions ---
    
    # 1. Initial Transaction (user 101) - Not fraud
    result_101 = [row for row in results if row.user_id == 101][0]
    assert result_101.is_fraudulent == False
    assert result_101.speed_kmh == 0.0 # Speed check skipped for first transaction

    # 2. FRAUD Transaction (user 102) - Should be flagged
    result_102 = [row for row in results if row.user_id == 102][0]
    assert result_102.is_fraudulent == True
    # The speed should be extremely high (approx 11140 km/h)
    assert result_102.speed_kmh > 10000 

    # 3. VALID Transaction (user 103) - Should NOT be flagged
    result_103 = [row for row in results if row.user_id == 103][0]
    assert result_103.is_fraudulent == False
    # The speed should be around 550 km/h (5570 km / 10 hours)
    assert 500 < result_103.speed_kmh < 600 


# Note: More advanced tests would simulate the actual mapGroupsWithState 
# logic by manually creating the state and processing multiple groups.