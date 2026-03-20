"""
pipeline/extract.py
Extracts hourly weather data from the Open-Meteo API for a set of cities.
Open-Meteo is completely free and requires no API key.
Docs: https://open-meteo.com/en/docs
"""

import requests
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Cities to track: name, latitude, longitude
CITIES = [
    {"name": "New York",  "lat": 40.71,  "lon": -74.01},
        {"name": "London",    "lat": 51.51,  "lon": -0.13},
            {"name": "Tokyo",     "lat": 35.68,  "lon": 139.69},
                {"name": "Sydney",    "lat": -33.87, "lon": 151.21},
                    {"name": "Lagos",     "lat": 6.52,   "lon": 3.38},
                    ]

                    OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

                    HOURLY_VARIABLES = [
                        "temperature_2m",
                            "apparent_temperature",
                                "relative_humidity_2m",
                                    "wind_speed_10m",
                                        "wind_direction_10m",
                                            "precipitation",
                                                "weather_code",
                                                ]


                                                def fetch_city(city: dict, days_back: int = 7) -> dict[str, Any]:
                                                    """
                                                        Fetch hourly weather data for a single city.
                                                            Uses the 'past_days' parameter to retrieve historical data.
                                                                Returns the raw API response dict.
                                                                    """
                                                                        params = {
                                                                                "latitude": city["lat"],
                                                                                        "longitude": city["lon"],
                                                                                                "hourly": ",".join(HOURLY_VARIABLES),
                                                                                                        "past_days": days_back,
                                                                                                                "forecast_days": 1,
                                                                                                                        "timezone": "UTC",
                                                                                                                            }
                                                                                                                            
                                                                                                                                try:
                                                                                                                                        response = requests.get(OPEN_METEO_URL, params=params, timeout=15)
                                                                                                                                                response.raise_for_status()
                                                                                                                                                        data = response.json()
                                                                                                                                                                logger.info("Fetched %d hours for %s", len(data.get("hourly", {}).get("time", [])), city["name"])
                                                                                                                                                                        return {"city": city, "data": data}
                                                                                                                                                                            except requests.exceptions.RequestException as e:
                                                                                                                                                                                    logger.error("Failed to fetch data for %s: %s", city["name"], e)
                                                                                                                                                                                            return {"city": city, "data": None, "error": str(e)}
                                                                                                                                                                                            
                                                                                                                                                                                            
                                                                                                                                                                                            def extract_all(days_back: int = 7) -> list[dict[str, Any]]:
                                                                                                                                                                                                """
                                                                                                                                                                                                    Extract data for all configured cities.
                                                                                                                                                                                                        Returns a list of raw result dicts.
                                                                                                                                                                                                            """
                                                                                                                                                                                                                logger.info("Starting extraction for %d cities, %d days back", len(CITIES), days_back)
                                                                                                                                                                                                                    results = [fetch_city(city, days_back) for city in CITIES]
                                                                                                                                                                                                                        successful = sum(1 for r in results if r.get("data") is not None)
                                                                                                                                                                                                                            logger.info("Extraction complete: %d/%d cities successful", successful, len(CITIES))
                                                                                                                                                                                                                                return results
                                                                                                                                                                                                                                
                                                                                                                                                                                                                                
                                                                                                                                                                                                                                if __name__ == "__main__":
                                                                                                                                                                                                                                    results = extract_all()
                                                                                                                                                                                                                                        for r in results:
                                                                                                                                                                                                                                                city_name = r["city"]["name"]
                                                                                                                                                                                                                                                        if r.get("data"):
                                                                                                                                                                                                                                                                    hours = len(r["data"]["hourly"]["time"])
                                                                                                                                                                                                                                                                                print(f"{city_name}: {hours} hourly records fetched")
                                                                                                                                                                                                                                                                                        else:
                                                                                                                                                                                                                                                                                                    print(f"{city_name}: FAILED - {r.get('error', 'unknown error')}")
