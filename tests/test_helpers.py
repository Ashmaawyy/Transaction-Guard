import pytest
from utils.helpers import haversine_distance_km, check_impossible_speed

# --- Test Case 1: Haversine Distance Calculation ---
def test_haversine_known_distance():
    """
    Test Haversine distance between two well-known cities.
    (New York City to London is approx 5570 km)
    """
    # NYC Coordinates
    lat1, lon1 = 40.7128, -74.0060 
    # London Coordinates
    lat2, lon2 = 51.5074, 0.1278 
    
    distance = haversine_distance_km(lat1, lon1, lat2, lon2)
    
    # Assert distance is within a small tolerance of the known value
    assert 5500 < distance < 5600

def test_haversine_zero_distance():
    """Test distance when the two points are identical (should be near zero)."""
    lat1, lon1 = 10.0, 10.0
    lat2, lon2 = 10.0, 10.0
    
    distance = haversine_distance_km(lat1, lon1, lat2, lon2)
    assert distance < 0.001 # Check if it's less than 1 meter

# --- Test Case 2: Impossible Speed Check ---
def test_impossible_speed_fraud_detected():
    """
    Test case where travel is impossible (high distance, low time).
    NYC (T1) to London (T2) in 30 minutes (0.5 hours).
    Speed needed: ~11,140 km/h. Max believable speed: 1200 km/h. -> Should be FRAUD.
    """
    lat1, lon1 = 40.7128, -74.0060  # NYC
    lat2, lon2 = 51.5074, 0.1278   # London
    
    # 30 minutes in milliseconds
    time_diff_ms = 30 * 60 * 1000 
    
    is_fraud, speed = check_impossible_speed(lat1, lon1, lat2, lon2, time_diff_ms, max_speed_kmh=1200)
    
    assert is_fraud == True
    assert speed > 10000.0 # Speed must be extremely high

def test_believable_speed_no_fraud():
    """
    Test case where travel is believable (low distance, high time).
    Traveling 100 km in 1 hour. Speed is 100 km/h. -> Should NOT be fraud.
    """
    # Two points 100 km apart (approximate coordinates)
    lat1, lon1 = 34.0, -118.0
    # Calculated 100km North
    lat2, lon2 = 34.8983, -118.0 

    # 1 hour in milliseconds
    time_diff_ms = 60 * 60 * 1000 
    
    is_fraud, speed = check_impossible_speed(lat1, lon1, lat2, lon2, time_diff_ms, max_speed_kmh=1200)
    
    assert is_fraud == False
    assert 90 < speed < 110 # Should be close to 100 km/h

def test_near_simultaneous_transaction():
    """
    Test edge case: two transactions in the same location (e.g., across the street) 
    with near-zero time difference.
    This should not be flagged by impossible speed, but we check the handling of time_diff_hours.
    """
    # Same location (Small distance)
    lat1, lon1 = 34.0, -118.0
    lat2, lon2 = 34.0001, -118.0001
    
    # 10 milliseconds time difference
    time_diff_ms = 10 
    
    is_fraud, speed = check_impossible_speed(lat1, lon1, lat2, lon2, time_diff_ms, max_speed_kmh=1200)
    
    # Distance is near zero, so even a tiny time makes the speed manageable, 
    # but the helper is designed to cap near-zero time to avoid insane spikes,
    # and in this case, the distance is tiny, so speed remains low.
    assert is_fraud == False