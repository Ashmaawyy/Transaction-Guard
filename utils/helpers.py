import time
from haversine import haversine, Unit
from typing import Tuple

# --- Constants ---
# Earth's radius in kilometers (for distance calculations)
EARTH_RADIUS_KM = 6371 
# Maximum believable speed (e.g., high-speed jet travel in km/h)
# 1200 km/h is roughly the speed of sound/supersonic jet.
MAX_BELIEVABLE_SPEED_KMH = 1200 

# --- Core Logic ---

def haversine_distance_km(
    lat1: float, lon1: float, lat2: float, lon2: float
) -> float:
    """
    Calculates the great-circle distance between two points on the Earth
    using the Haversine formula.

    Args:
        lat1, lon1: Latitude and longitude of the first point (e.g., last known location).
        lat2, lon2: Latitude and longitude of the second point (e.g., new transaction location).

    Returns:
        The distance in kilometers (float).
    """
    point1 = (lat1, lon1)
    point2 = (lat2, lon2)
    
    # The haversine function takes care of the math
    distance = haversine(point1, point2, unit=Unit.KILOMETERS)
    return distance

def check_impossible_speed(
    last_lat: float, last_lon: float, 
    current_lat: float, current_lon: float, 
    time_diff_ms: int, 
    max_speed_kmh: float = MAX_BELIEVABLE_SPEED_KMH
) -> Tuple[bool, float]:
    """
    Determines if the distance traveled between two transactions, given the
    time difference, exceeds a realistic maximum speed.

    Args:
        last_lat, last_lon: Geospatial coordinates of the previous transaction.
        current_lat, current_lon: Geospatial coordinates of the current transaction.
        time_diff_ms: Time difference between transactions in milliseconds.
        max_speed_kmh: The threshold speed (e.g., 1200 km/h) to flag.

    Returns:
        A tuple: (is_impossible_speed: bool, calculated_speed_kmh: float).
    """
    
    # 1. Calculate Distance
    distance_km = haversine_distance_km(
        last_lat, last_lon, current_lat, current_lon
    )
    
    # 2. Convert Time Difference
    # Convert milliseconds to hours
    time_diff_hours = time_diff_ms / (1000 * 60 * 60)
    
    # Handle near-zero time difference to avoid division by zero and unrealistic speed spikes
    if time_diff_hours < 0.0001: # 0.36 seconds
        # If time difference is too small, assume no meaningful travel speed can be calculated
        # or it's a simultaneous swipe, which often triggers fraud.
        # We cap the speed calculation here and rely on other rules, but still return a high speed.
        calculated_speed_kmh = max_speed_kmh * 2 
    else:
        # 3. Calculate Speed (Speed = Distance / Time)
        calculated_speed_kmh = distance_km / time_diff_hours

    # 4. Check for Impossibility
    is_fraud = calculated_speed_kmh > max_speed_kmh
    
    return is_fraud, calculated_speed_kmh

# --- Other Utilities ---

def get_current_time_ms() -> int:
    """Returns the current Unix timestamp in milliseconds."""
    return int(time.time() * 1000)
