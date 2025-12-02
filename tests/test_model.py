import pytest
from pydantic import ValidationError
from utils.model import Transaction

# --- Test Case 1: Valid Transaction ---
def test_valid_transaction_creation():
    """Test that a transaction with valid, normal data is created successfully."""
    valid_data = {
        "user_id": 1001,
        "amount": 55.75,
        "merchant_id": "AMAZON_WEB",
        "latitude": 34.0522,  # Los Angeles
        "longitude": -118.2437,
    }
    # Should not raise any exception
    txn = Transaction(**valid_data)
    assert txn.user_id == 1001
    assert txn.currency == "USD"
    # The transaction_id and timestamp_ms should be auto-generated
    assert isinstance(txn.transaction_id, str)
    assert isinstance(txn.timestamp_ms, int)

# --- Test Case 2: Invalid Amount ---
def test_invalid_amount_rejection():
    """Test that transactions with zero or negative amounts are rejected."""
    bad_data = {
        "user_id": 1002,
        "amount": -0.01,  # Invalid amount
        "merchant_id": "THEFT_R_US",
        "latitude": 0.0,
        "longitude": 0.0,
    }
    with pytest.raises(ValidationError) as excinfo:
        Transaction(**bad_data)
    
    # Check if the error message mentions the 'amount' field
    assert 'Transaction amount must be positive' in str(excinfo.value)

# --- Test Case 3: Invalid Latitude ---
def test_invalid_latitude_rejection():
    """Test that transactions with latitude outside of [-90, 90] are rejected."""
    bad_data = {
        "user_id": 1003,
        "amount": 10.00,
        "merchant_id": "FAR_NORTH",
        "latitude": 90.0001, # Invalid latitude
        "longitude": 10.0,
    }
    with pytest.raises(ValidationError) as excinfo:
        Transaction(**bad_data)
    
    assert 'Latitude must be between -90 and 90' in str(excinfo.value)

# --- Test Case 4: Invalid Longitude ---
def test_invalid_longitude_rejection():
    """Test that transactions with longitude outside of [-180, 180] are rejected."""
    bad_data = {
        "user_id": 1004,
        "amount": 20.00,
        "merchant_id": "FAR_EAST",
        "latitude": 10.0,
        "longitude": -180.0001, # Invalid longitude
    }
    with pytest.raises(ValidationError) as excinfo:
        Transaction(**bad_data)
        
    assert 'Longitude must be between -180 and 180' in str(excinfo.value)