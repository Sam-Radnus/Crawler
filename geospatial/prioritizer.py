import re
import json
import os
import requests
from bs4 import BeautifulSoup
from typing import Literal, Optional
from geopy.geocoders import Nominatim

Priority = Literal[1, 2, 3, 4, 5]


class Prioritizer:
    """Assigns priorities to URLs based on domain and URL type."""

    def __init__(self):
        self.listing_patterns = [r"/search/apa"]
        self.geolocator = Nominatim(user_agent="state_locator", timeout=10)
        self.counter = 0  # in order to do round robin priority assignment

        # Get the directory where this file is located
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self.cache_file = os.path.join(current_dir, "state_coords.json")

        # Step 1: load coordinate cache
        self.state_coords = self._load_cache()

        # Step 2: fetch craigslist sites
        url = "https://www.craigslist.org/about/sites"
        soup = BeautifulSoup(requests.get(url).text, "html.parser")

        us_states = [
            "Alabama",
            "Alaska",
            "Arizona",
            "Arkansas",
            "California",
            "Colorado",
            "Connecticut",
            "Delaware",
            "Florida",
            "Georgia",
            "Hawaii",
            "Idaho",
            "Illinois",
            "Indiana",
            "Iowa",
            "Kansas",
            "Kentucky",
            "Louisiana",
            "Maine",
            "Maryland",
            "Massachusetts",
            "Michigan",
            "Minnesota",
            "Mississippi",
            "Missouri",
            "Montana",
            "Nebraska",
            "Nevada",
            "New Hampshire",
            "New Jersey",
            "New Mexico",
            "New York",
            "North Carolina",
            "North Dakota",
            "Ohio",
            "Oklahoma",
            "Oregon",
            "Pennsylvania",
            "Rhode Island",
            "South Carolina",
            "South Dakota",
            "Tennessee",
            "Texas",
            "Utah",
            "Vermont",
            "Virginia",
            "Washington",
            "West Virginia",
            "Wisconsin",
            "Wyoming",
        ]

        # Step 3: extract city links per state
        sites = {}
        for state_h4 in soup.find_all("h4"):
            state = state_h4.text.strip()
            sites[state] = [a["href"]
                            for a in state_h4.find_next_sibling("ul").find_all("a")]
        sites = {k: v for k, v in sites.items() if k in us_states}

        # Step 4: build city→state map
        self.map_cities_to_states = {}
        for state, cities in sites.items():
            for city in cities:
                city_name = city.split("//")[1].split(".")[0]
                self.map_cities_to_states[city_name] = state

        # Step 5: ensure coordinates for all states exist
        self._populate_missing_coords(us_states)

        # Step 6: region division by longitude
        us_states.sort(key=lambda s: self.state_coords[s])
        pivot = len(us_states) // 3
        self.regions = {s: min(i // pivot, 2) for i, s in enumerate(us_states)}

    # --- internal helpers ---

    def _load_cache(self):
        if os.path.exists(self.cache_file):
            with open(self.cache_file) as f:
                data = json.load(f)
                # Convert [lat, lon] arrays to just longitude values
                return {
                    k: v[1] if isinstance(
                        v,
                        list) else v for k,
                    v in data.items()}
        return {}

    def _save_cache(self):
        # Save as [lat, lon] arrays for compatibility
        data_to_save = {k: [v, 0] if not isinstance(v, list) else v
                        for k, v in self.state_coords.items()}
        with open(self.cache_file, "w") as f:
            json.dump(data_to_save, f, indent=2)

    def _populate_missing_coords(self, states):
        updated = False
        for state in states:
            if state not in self.state_coords:
                loc = self.geolocator.geocode(f"{state}, USA")
                if loc:
                    self.state_coords[state] = loc.longitude
                    updated = True
                else:
                    self.state_coords[state] = 0  # fallback
        if updated:
            self._save_cache()

    # --- public methods ---

    def is_target_domain(self, url: str) -> bool:
        return "craigslist.org" in url.lower()

    def is_listing_page(self, url: str) -> bool:
        return any(re.search(p, url, re.IGNORECASE)
                   for p in self.listing_patterns)

    def assign_priority(self, url: str) -> Optional[Priority]:
        if not self.is_target_domain(url):
            return -1
        try:
            city = url.split("//")[1].split(".")[0]
            state = self.map_cities_to_states[city]
            region = self.regions[state]
            if self.is_listing_page(url):
                self.counter += 1
                return 1 if self.counter % 2 == 0 else 2
            return region + 3
        except (IndexError, KeyError):
            return -1
