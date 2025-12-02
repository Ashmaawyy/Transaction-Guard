import time
from uuid import uuid4
from pydantic import BaseModel, Field, validator
from typing import Optional

class Transaction(BaseModel):
    """
    Defines the schema for a financial transaction.
    If data doesn't match this, it won't even be sent to Kafka.
    """
    transaction_id: str = Field(default_factory=lambda: str(uuid4()))
    user_id: int
    amount: float
    currency: str = "USD"
    timestamp_ms: int = Field(default_factory=lambda: int(time.time() * 1000))
    merchant_id: str
    # Geospatial data for our "Impossible Speed" check
    latitude: float
    longitude: float

    @validator('amount')
    def amount_must_be_positive(cls, v):
        if v <= 0:
            raise ValueError('Transaction amount must be positive')
        return v

    @validator('latitude')
    def valid_latitude(cls, v):
        if not (-90 <= v <= 90):
            raise ValueError('Latitude must be between -90 and 90')
        return v
