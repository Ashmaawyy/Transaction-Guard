import time
import json
import random
import sys
from kafka import KafkaProducer
from faker import Faker
from datetime import datetime
from pydantic import ValidationError

# Ensure the parent directory (project root) is in the path to import utils
sys.path.append('.') 
from utils.model import Transaction # Import the Pydantic schema
from utils.helpers import get_current_time_ms # Import the utility for current time

# --- Configuration ---
KAFKA_BROKER = 'localhost:9092' # Matches the advertised listener in docker-compose
KAFKA_TOPIC = 'financial_transactions'
SLEEP_TIME_SECONDS = 0.05 # Fast stream: 20 events per second
NUM_USERS = 1000 # Simulate transactions for 1000 unique users
MAX_NORMAL_AMOUNT = 500.00
FRAUD_RATE = 0.10 # 10% chance of a potential fraud transaction

# Initialize Faker
fake = Faker()

# --- Kafka Setup ---
try:
    # Initialize the Kafka Producer
    # Use json.dumps as the value serializer to send structured data
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # Optional: ensure message delivery
        acks='all',
        retries=3,
        # Timeout settings
        request_timeout_ms=10000,
        api_version=(0, 10)
    )
except Exception as e:
    print(f"ERROR: Could not connect to Kafka Broker at {KAFKA_BROKER}. Ensure docker-compose is running.", file=sys.stderr)
    sys.exit(1)


# --- Utility Functions ---

def generate_user_id():
    """Generates a random user ID for the transaction, simulating repeat users."""
    return random.randint(100000, 100000 + NUM_USERS)

def generate_transaction_data(user_id: int) -> dict[str, any]:
    """
    Creates a single transaction event dictionary compliant with the Pydantic schema.
    Simulates both valid and potentially fraudulent data (for testing the processor).
    """
    
    is_normal = random.random() > FRAUD_RATE
    
    if is_normal:
        # Normal transaction: small amount, standard location near the center of a continent
        amount = round(random.uniform(1.00, MAX_NORMAL_AMOUNT), 2)
        # Random location within a common area (e.g., North America/Europe range)
        lat = random.uniform(25.0, 60.0) 
        lon = random.uniform(-100.0, 50.0)
    else:
        # Potential fraud simulation: very high amount and/or impossible coordinates
        # 50% chance of impossible amount, 50% chance of extreme, impossible coordinates
        if random.random() < 0.5:
            # Type 1 Fraud: High amount
            amount = round(random.uniform(MAX_NORMAL_AMOUNT * 5, MAX_NORMAL_AMOUNT * 10), 2)
            lat = fake.latitude()
            lon = fake.longitude()
        else:
            # Type 2 Fraud: Impossible Coordinates (Will be caught by Pydantic if out of range)
            # We simulate a move far away from the last known location, but within valid range
            amount = round(random.uniform(10.00, 100.00), 2)
            lat = fake.latitude() # Random valid location 1
            lon = fake.longitude() # Random valid location 2 (simulating a jump from the last state)

    
    # Use the Pydantic model for strict validation before sending
    try:
        transaction = Transaction(
            user_id=user_id,
            amount=amount,
            merchant_id=fake.company(),
            latitude=lat,
            longitude=lon,
            # transaction_id and timestamp_ms are auto-generated defaults
        )
        return transaction.model_dump()
    except ValidationError as e:
        # This should rarely happen if the Faker coordinates are set correctly,
        # but serves as a crucial defensive programming check.
        print(f"Schema Validation Error on Generated Data: {e}", file=sys.stderr)
        return None


# --- Main Loop ---

print(f"--- Starting Transaction Producer to {KAFKA_TOPIC} on {KAFKA_BROKER} ---")
print(f"Data Rate: {1/SLEEP_TIME_SECONDS} events/second. Fraud Rate: {FRAUD_RATE*100:.0f}%")

try:
    while True:
        user = generate_user_id()
        data = generate_transaction_data(user)
        
        if data:
            # Send the transaction data to Kafka
            future = producer.send(
                topic=KAFKA_TOPIC, 
                key=str(user).encode('utf-8'), # Key by User ID for partitioning and stateful processing
                value=data
            )
            
            # Non-blocking logging of success/failure
            # You can uncomment this for stricter error monitoring:
            # try:
            #     future.get(timeout=1)
            # except Exception as e:
            #     print(f"Kafka Send Error: {e}", file=sys.stderr)
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Sent Txn {data['transaction_id']} (User: {data['user_id']}, Amount: ${data['amount']:.2f})")

        time.sleep(SLEEP_TIME_SECONDS)

except KeyboardInterrupt:
    print("\nProducer stopped by user.")
except Exception as e:
    print(f"An unexpected error occurred: {e}", file=sys.stderr)
finally:
    producer.flush()
    producer.close()
    print("Kafka Producer closed.")
