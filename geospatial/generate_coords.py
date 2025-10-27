import json
import os
import time
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable

CACHE_FILE = "state_coords.json"

US_STATES = [
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
    "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho",
    "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana",
    "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
    "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
    "New Hampshire", "New Jersey", "New Mexico", "New York",
    "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon",
    "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota",
    "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington",
    "West Virginia", "Wisconsin", "Wyoming"
]


def get_latitude(geolocator, state, retries=3):
    """Fetch latitude for a given state with retry logic."""
    for attempt in range(retries):
        try:
            location = geolocator.geocode(f"{state}, USA", timeout=10)
            if location:
                return location.latitude,location.longitude
        except (GeocoderTimedOut, GeocoderUnavailable):
            time.sleep(2)  # backoff before retry
    return None


def main():
    geolocator = Nominatim(user_agent="state_locator", timeout=10)

    # Load existing cache if present
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE) as f:
            try:
                state_coords = json.load(f)
            except json.JSONDecodeError:
                print(f"Warning: {CACHE_FILE} is empty or contains invalid JSON. Starting with an empty cache.")
                state_coords = {}
    else:
        state_coords = {}

    for state in US_STATES:
        if state not in state_coords:
            print(f"Fetching latitude for {state}...")
            lat,lon = get_latitude(geolocator, state)
            if lat is not None:
                state_coords[state] = [lat,lon]
                print(f"→ {state}: {lat},{lon}")
            else:
                print(f"Failed to fetch {state}")
            time.sleep(1)  # respect API rate limits

    with open(CACHE_FILE, "w") as f:
        json.dump(state_coords, f, indent=2)
    print(f"\nSaved {len(state_coords)} states to {CACHE_FILE}")


if __name__ == "__main__":
    main()
