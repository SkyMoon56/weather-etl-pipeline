"""
tests/test_transform.py
Unit tests for pipeline/transform.py
Run with: pytest tests/
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pytest
import pandas as pd
from pipeline.transform import (
    celsius_to_fahrenheit,
    calc_heat_index,
    calc_wind_chill,
    code_to_category,
    transform_city,
    transform_all,
)


# ── celsius_to_fahrenheit ────────────────────────────────────────────────────

def test_celsius_to_fahrenheit_freezing():
    assert celsius_to_fahrenheit(0) == 32.0

def test_celsius_to_fahrenheit_boiling():
    assert celsius_to_fahrenheit(100) == 212.0

def test_celsius_to_fahrenheit_body_temp():
    assert celsius_to_fahrenheit(37) == pytest.approx(98.6, abs=0.1)

def test_celsius_to_fahrenheit_nan():
    assert celsius_to_fahrenheit(float("nan")) is None


# ── calc_heat_index ──────────────────────────────────────────────────────────

def test_heat_index_not_applicable_cold():
    # Below threshold (< 27C), should return None
    assert calc_heat_index(20, 80) is None

def test_heat_index_not_applicable_low_humidity():
    # Low humidity, should return None
    assert calc_heat_index(35, 20) is None

def test_heat_index_hot_humid():
    # Hot + humid should return a value
    result = calc_heat_index(35, 80)
    assert result is not None
    assert result > 35  # feels hotter than actual temp

def test_heat_index_nan_temp():
    assert calc_heat_index(float("nan"), 80) is None


# ── calc_wind_chill ──────────────────────────────────────────────────────────

def test_wind_chill_not_applicable_warm():
    # Temp > 10C, not applicable
    assert calc_wind_chill(15, 20) is None

def test_wind_chill_not_applicable_calm():
    # Wind < 4.8 km/h, not applicable
    assert calc_wind_chill(0, 2) is None

def test_wind_chill_cold_and_windy():
    # -10C with 30 km/h wind should feel much colder
    result = calc_wind_chill(-10, 30)
    assert result is not None
    assert result < -10

def test_wind_chill_nan():
    assert calc_wind_chill(float("nan"), 30) is None


# ── code_to_category ─────────────────────────────────────────────────────────

def test_code_clear():
    assert code_to_category(0) == "Clear"

def test_code_rain():
    assert code_to_category(61) == "Light Rain"

def test_code_thunderstorm():
    assert code_to_category(95) == "Thunderstorm"

def test_code_unknown():
    assert "50" in code_to_category(50)  # Unknown code -> "Code 50"

def test_code_nan():
    assert code_to_category(float("nan")) == "Unknown"


# ── transform_city ───────────────────────────────────────────────────────────

MOCK_CITY = {"name": "Test City", "lat": 10.0, "lon": 20.0}

MOCK_DATA = {
    "hourly": {
        "time":                  ["2024-01-01T00:00", "2024-01-01T01:00"],
        "temperature_2m":        [25.0, 26.0],
        "apparent_temperature":  [24.0, 25.5],
        "relative_humidity_2m":  [60, 65],
        "wind_speed_10m":        [10.0, 12.0],
        "wind_direction_10m":    [180, 200],
        "precipitation":         [0.0, 0.5],
        "weather_code":          [0, 61],
    }
}

def test_transform_city_returns_dataframe():
    raw = {"city": MOCK_CITY, "data": MOCK_DATA}
    df = transform_city(raw, "2024-01-01T00:00:00+00:00")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2

def test_transform_city_columns():
    raw = {"city": MOCK_CITY, "data": MOCK_DATA}
    df = transform_city(raw, "2024-01-01T00:00:00+00:00")
    for col in ["city", "temp_c", "temp_f", "weather_category", "ingested_at"]:
        assert col in df.columns

def test_transform_city_temp_f_correct():
    raw = {"city": MOCK_CITY, "data": MOCK_DATA}
    df = transform_city(raw, "2024-01-01T00:00:00+00:00")
    assert df["temp_f"].iloc[0] == pytest.approx(77.0, abs=0.1)

def test_transform_city_no_data():
    raw = {"city": MOCK_CITY, "data": None}
    result = transform_city(raw, "2024-01-01T00:00:00+00:00")
    assert result is None

def test_transform_all_combines_cities():
    raw_results = [
        {"city": MOCK_CITY, "data": MOCK_DATA},
        {"city": {"name": "City B", "lat": 50.0, "lon": 10.0}, "data": MOCK_DATA},
    ]
    df = transform_all(raw_results)
    assert len(df) == 4  # 2 rows per city
    assert set(df["city"].unique()) == {"Test City", "City B"}

def test_transform_all_skips_failed_cities():
    raw_results = [
        {"city": MOCK_CITY, "data": MOCK_DATA},
        {"city": {"name": "Bad City", "lat": 0, "lon": 0}, "data": None},
    ]
    df = transform_all(raw_results)
    assert len(df) == 2
    assert "Bad City" not in df["city"].values
