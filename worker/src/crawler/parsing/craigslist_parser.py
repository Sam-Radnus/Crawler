"""
Craigslist Property Parser - Extract property data from Craigslist HTML
"""

import re
from typing import Dict, Any
from bs4 import BeautifulSoup
from datetime import datetime


def parse_craigslist_property(html_content: str, url: str) -> Dict[str, Any]:
    """
    Parse Craigslist property page HTML and extract property information.

    Args:
        html_content: HTML content of the Craigslist property page
        url: URL of the property page

    Returns:
        Dictionary containing parsed property data with keys:
        - title: Property title
        - address: Property address
        - price: Price (integer)
        - latitude: Latitude coordinate
        - longitude: Longitude coordinate
        - property_type: Type of property (apartment, house, etc.)
        - beds: Number of bedrooms
        - baths: Number of bathrooms
        - sqft: Square footage
        - posted_date: Date when posted
        - city: City name
    """
    soup = BeautifulSoup(html_content, 'html.parser')

    # Initialize result dictionary with defaults
    result: Dict[str, Any] = {
        'title': None,
        'address': None,
        'price': None,
        'latitude': None,
        'longitude': None,
        'property_type': None,
        'beds': None,
        'baths': None,
        'sqft': None,
        'posted_date': None,
        'city': None
    }

    try:
        # Extract title
        title_elem = soup.find(
            'span', id='titletextonly') or soup.find(
            'h1', class_='postingtitletext')
        if title_elem:
            result['title'] = title_elem.get_text().strip()

        # Extract price
        price_elem = soup.find(
            'span', class_='price') or soup.find(
            'span', class_='priceblock')
        if price_elem:
            price_text = price_elem.get_text().strip()
            # Extract numbers from price string (e.g., "$1,500" -> 1500)
            price_match = re.search(r'[\d,]+', price_text.replace(',', ''))
            if price_match:
                result['price'] = int(price_match.group().replace(',', ''))

        # Extract address
        mapbox_elem = soup.find('div', id='mapbox')
        if mapbox_elem:
            address_elem = mapbox_elem.find('div', class_='mapaddress')
            if address_elem:
                result['address'] = address_elem.get_text().strip()

        # Extract coordinates from mapbox data attribute
        mapbox = soup.find('div', id='mapbox')
        if mapbox:
            # Look for data-latitude and data-longitude attributes
            if 'data-latitude' in mapbox.attrs:
                try:
                    result['latitude'] = float(mapbox['data-latitude'])
                except (ValueError, TypeError):
                    pass
            if 'data-longitude' in mapbox.attrs:
                try:
                    result['longitude'] = float(mapbox['data-longitude'])
                except (ValueError, TypeError):
                    pass

            # Alternative: look for coordinates in JavaScript variables
            if result['latitude'] is None or result['longitude'] is None:
                coord_match = re.search(
                    r'data-latitude="([^"]+)"[^>]*data-longitude="([^"]+)"', str(mapbox))
                if coord_match:
                    try:
                        result['latitude'] = float(coord_match.group(1))
                        result['longitude'] = float(coord_match.group(2))
                    except (ValueError, TypeError):
                        pass

        # Extract property attributes (beds, baths, sqft)
        attr_group = soup.find('span', class_='shared-line-bubble')
        if attr_group:
            # Look for patterns like "2br", "1ba", "1200ft2"
            text = attr_group.get_text()

            # Extract bedrooms
            br_match = re.search(r'(\d+)\s*br', text, re.IGNORECASE)
            if br_match:
                result['beds'] = int(br_match.group(1))

            # Extract bathrooms
            ba_match = re.search(r'(\d+(?:\.\d+)?)\s*ba', text, re.IGNORECASE)
            if ba_match:
                try:
                    result['baths'] = float(ba_match.group(1))
                except ValueError:
                    pass

            # Extract square footage
            sqft_match = re.search(
                r'(\d+(?:,\d+)?)\s*ft[²2]', text, re.IGNORECASE)
            if sqft_match:
                result['sqft'] = int(sqft_match.group(1).replace(',', ''))

        # Alternative: try to find attributes in separate spans
        if result['beds'] is None:
            beds_elem = soup.find(
                'span', string=re.compile(
                    r'\d+\s*br', re.IGNORECASE))
            if beds_elem:
                beds_match = re.search(
                    r'(\d+)\s*br', beds_elem.get_text(), re.IGNORECASE)
                if beds_match:
                    result['beds'] = int(beds_match.group(1))

        if result['baths'] is None:
            baths_elem = soup.find(
                'span', string=re.compile(
                    r'\d+\s*ba', re.IGNORECASE))
            if baths_elem:
                baths_match = re.search(
                    r'(\d+(?:\.\d+)?)\s*ba',
                    baths_elem.get_text(),
                    re.IGNORECASE)
                if baths_match:
                    try:
                        result['baths'] = float(baths_match.group(1))
                    except ValueError:
                        pass

        if result['sqft'] is None:
            sqft_elem = soup.find(
                'span', string=re.compile(
                    r'\d+\s*ft', re.IGNORECASE))
            if sqft_elem:
                sqft_match = re.search(
                    r'(\d+(?:,\d+)?)\s*ft[²2]',
                    sqft_elem.get_text(),
                    re.IGNORECASE)
                if sqft_match:
                    result['sqft'] = int(sqft_match.group(1).replace(',', ''))

        # Extract property type
        if 'apa' in url.lower():
            result['property_type'] = 'apartment'
        elif 'rea' in url.lower():
            result['property_type'] = 'real estate'

        # Extract posted date
        time_elem = soup.find(
            'time', class_='date') or soup.find(
            'time', attrs={
                'datetime': True})
        if time_elem:
            datetime_attr = time_elem.get('datetime')
            if datetime_attr:
                try:
                    # Parse ISO format datetime (handle timezone if present)
                    if datetime_attr.endswith('Z'):
                        datetime_attr = datetime_attr.replace('Z', '+00:00')
                    result['posted_date'] = datetime.fromisoformat(
                        datetime_attr)
                except (ValueError, AttributeError):
                    pass

        # Extract city from URL
        city_match = re.search(r'https?://([^.]+)\.craigslist\.org', url)
        if city_match:
            result['city'] = city_match.group(1)

    except Exception as e:
        # If parsing fails, return what we have with error info
        result['_parse_error'] = str(e)

    return result
