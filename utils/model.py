from uuid import uuid4
from pydantic import BaseModel, Field, validator
import time
from typing import Optional

class Transaction(BaseModel):
    """
    Defines the schema for a financial transaction.
    All fields are mandatory and validated against physical limits.
    """
    transaction_id: str = Field(default_factory=lambda: str(uuid4()))
    user_id: int
    amount: float
    currency: str = "USD"
    # Unix timestamp in milliseconds
    timestamp_ms: int = Field(default_factory=lambda: int(time.time() * 1000))
    merchant_id: str
    
    # Geospatial data fields
    latitude: float
    longitude: float

    @validator('amount')
    def amount_must_be_positive(cls, v):
        """Ensures the transaction amount is realistic."""
        if v <= 0:
            raise ValueError('Transaction amount must be positive')
        return v

    @validator('latitude')
    def valid_latitude(cls, v):
        """Validates that latitude is within the physical range of -90 to +90 degrees."""
        if not (-90 <= v <= 90):
            raise ValueError('Latitude must be between -90 and 90')
        return v

    @validator('longitude')
    def valid_longitude(cls, v):
        """Validates that longitude is within the physical range of -180 to +180 degrees."""
        if not (-180 <= v <= 180):
            raise ValueError('Longitude must be between -180 and 180')
        return v

# Example of the enriched data structure after Spark processing
class EnrichedTransaction(Transaction):
    is_fraudulent: bool
    fraud_reason: str