from bs4 import BeautifulSoup
import requests
import json
from geopy.geocoders import Nominatim
import random


def get_state_lat_long(state_name):
    geolocator = Nominatim(user_agent="state_locator")
    location = geolocator.geocode(state_name + ", USA")
    if location:
        return (location.latitude, location.longitude)
    else:
        return None


# Fetch Craigslist site URLs
url = "https://www.craigslist.org/about/sites"
soup = BeautifulSoup(requests.get(url).text, "html.parser")

us_states = [
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

# Extract sites
sites = {}
for state_h4 in soup.find_all("h4"):
    state = state_h4.text.strip()
    sites[state] = [a['href']
                    for a in state_h4.find_next_sibling("ul").find_all("a")]

sites = {k: v for k, v in sites.items() if k in us_states}

# Generate city search URLs
seed_urls = []
for state, cities in sites.items():
    for city in cities:
        seed_urls.append(city + "/search/apa#search=2~gallery~0")

# Load config.json
with open("config.json", "r") as f:
    config = json.load(f)

random.shuffle(seed_urls)
# Update seed_urls
config["seed_urls"] = seed_urls

# Write back to config.json
with open("config.json", "w") as f:
    json.dump(config, f, indent=4)

print(f"Added {len(seed_urls)} URLs to config.json")
