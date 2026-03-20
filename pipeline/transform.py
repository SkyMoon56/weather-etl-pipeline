"""
pipeline/transform.py
Transforms raw Open-Meteo API responses into clean, enriched pandas DataFrames.
"""

import pandas as pd
import math
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# WMO Weather code -> human-readable category
WMO_CATEGORIES = {
    0:  "Clear",
    1:  "Mostly Clear", 2: "Partly Cloudy", 3: "Overcast",
    45: "Foggy",        48: "Icy Fog",
    51: "Light Drizzle",53: "Drizzle",      55: "Heavy Drizzle",
    61: "Light Rain",   63: "Rain",         65: "Heavy Rain",
    71: "Light Snow",   73: "Snow",         75: "Heavy Snow",
    77: "Snow Grains",
    80: "Light Showers",81: "Showers",      82: "Heavy Showers",
    85: "Snow Showers", 86: "Heavy Snow Showers",
    95: "Thunderstorm", 96: "Hail Storm",   99: "Heavy Hail Storm",
}


def code_to_category(code) -> str:
    if pd.isna(code):
        return "Unknown"
    return WMO_CATEGORIES.get(int(code), f"Code {int(code)}")


def celsius_to_fahrenheit(c) -> float | None:
    if pd.isna(c):
        return None
    return round(c * 9 / 5 + 32, 2)


def calc_heat_index(temp_c, humidity_pct) -> float | None:
    """Rothfusz heat index formula (valid for temp >= 27C, humidity >= 40%)."""
    if pd.isna(temp_c) or pd.isna(humidity_pct):
        return None
    T = temp_c * 9 / 5 + 32  # convert to F for formula
    R = humidity_pct
    if T < 80 or R < 40:
        return None
    HI = (-42.379 + 2.04901523*T + 10.14333127*R
          - 0.22475541*T*R - 0.00683783*T*T
          - 0.05481717*R*R + 0.00122874*T*T*R
          + 0.00085282*T*R*R - 0.00000199*T*T*R*R)
    return round((HI - 32) * 5 / 9, 2)  # back to Celsius


def calc_wind_chill(temp_c, wind_speed_kmh) -> float | None:
    """Wind chill formula (valid for temp <= 10C, wind > 4.8 km/h)."""
    if pd.isna(temp_c) or pd.isna(wind_speed_kmh):
        return None
    if temp_c > 10 or wind_speed_kmh < 4.8:
        return None
    wc = (13.12 + 0.6215 * temp_c
          - 11.37 * (wind_speed_kmh ** 0.16)
          + 0.3965 * temp_c * (wind_speed_kmh ** 0.16))
    return round(wc, 2)


def transform_city(raw_result: dict[str, Any], ingested_at: str) -> pd.DataFrame | None:
    """
    Transform one city's raw API result into a clean DataFrame.
    Returns None if data is missing.
    """
    city_meta = raw_result["city"]
    data = raw_result.get("data")

    if data is None:
        logger.warning("Skipping %s (no data)", city_meta["name"])
        return None

    hourly = data.get("hourly", {})
    if not hourly.get("time"):
        logger.warning("No hourly time data for %s", city_meta["name"])
        return None

    df = pd.DataFrame({
        "observed_at":       hourly.get("time", []),
        "temp_c":            hourly.get("temperature_2m", []),
        "feels_like_c":      hourly.get("apparent_temperature", []),
        "humidity_pct":      hourly.get("relative_humidity_2m", []),
        "wind_speed_kmh":    hourly.get("wind_speed_10m", []),
        "wind_direction_deg":hourly.get("wind_direction_10m", []),
        "precipitation_mm":  hourly.get("precipitation", []),
        "weather_code":      hourly.get("weather_code", []),
    })

    # Drop rows where we have no temperature at all
    df = df.dropna(subset=["temp_c"])

    # Derived columns
    df["temp_f"]           = df["temp_c"].apply(celsius_to_fahrenheit)
    df["weather_category"] = df["weather_code"].apply(code_to_category)
    df["heat_index_c"]     = df.apply(lambda r: calc_heat_index(r["temp_c"], r["humidity_pct"]), axis=1)
    df["wind_chill_c"]     = df.apply(lambda r: calc_wind_chill(r["temp_c"], r["wind_speed_kmh"]), axis=1)

    # Metadata
    df["city"]        = city_meta["name"]
    df["latitude"]    = city_meta["lat"]
    df["longitude"]   = city_meta["lon"]
    df["ingested_at"] = ingested_at

    # Reorder columns cleanly
    df = df[[
        "city", "latitude", "longitude", "observed_at",
        "temp_c", "temp_f", "feels_like_c", "humidity_pct",
        "wind_speed_kmh", "wind_direction_deg", "precipitation_mm",
        "weather_code", "weather_category", "heat_index_c", "wind_chill_c",
        "ingested_at",
    ]]

    logger.info("Transformed %d rows for %s", len(df), city_meta["name"])
    return df


def transform_all(raw_results: list[dict[str, Any]]) -> pd.DataFrame:
    """
    Transform all raw city results and combine into one DataFrame.
    """
    ingested_at = datetime.now(timezone.utc).isoformat()
    frames = [transform_city(r, ingested_at) for r in raw_results]
    frames = [f for f in frames if f is not None]

    if not frames:
        logger.error("No data to transform!")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    logger.info("Total transformed rows: %d", len(combined))
    return combined
