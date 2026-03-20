# 🌤️ Weather ETL Pipeline

A production-style **data engineering portfolio project** demonstrating a full Extract → Transform → Load pipeline using 100% free tools and APIs.

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![SQLite](https://img.shields.io/badge/Storage-SQLite-lightblue?logo=sqlite)
![GitHub Actions](https://img.shields.io/badge/Scheduler-GitHub_Actions-black?logo=githubactions)
![License](https://img.shields.io/badge/License-MIT-green)

---

## 📐 Architecture

```
Open-Meteo API (free, no key)
 │
 ▼
[ Extract ]  pipeline/extract.py
  Fetches hourly weather for 5 cities
 │
 ▼
[ Transform ]  pipeline/transform.py
  Cleans nulls, converts units, adds derived columns
  (heat index, wind chill, weather category)
 │
 ▼
[ Load ]  pipeline/load.py
  Upserts into SQLite (weather.db)
 │
 ▼
[ Analyze ]  pipeline/analyze.py
  Generates summary stats → reports/daily_summary.md
 │
 ▼
GitHub Actions (runs daily at 08:00 UTC)
  Commits updated DB + report back to repo
```

---

## 🗂️ Project Structure

```
weather-etl-pipeline/
├── pipeline/
│   ├── extract.py        # Pulls data from Open-Meteo API
│   ├── transform.py      # Cleans and enriches raw data
│   ├── load.py           # Loads into SQLite
│   ├── analyze.py        # Generates analytics report
│   └── run_pipeline.py   # Orchestrates the full ETL run
├── schema/
│   └── init_db.py        # Creates/migrates the SQLite schema
├── reports/
│   └── daily_summary.md  # Auto-generated daily report (committed by CI)
├── tests/
│   └── test_transform.py # Unit tests for transform logic
├── .github/
│   └── workflows/
│       └── daily_pipeline.yml  # GitHub Actions scheduler
├── requirements.txt
└── README.md
```

---

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- No API keys needed — Open-Meteo is completely free

### Installation

```bash
git clone https://github.com/SkyMoon56/weather-etl-pipeline.git
cd weather-etl-pipeline
pip install -r requirements.txt
```

### Run the full pipeline

```bash
python pipeline/run_pipeline.py
```

This will:
1. Fetch the last 7 days of hourly weather data for 5 cities
2. Transform and clean the data
3. Load it into `weather.db` (SQLite)
4. Generate `reports/daily_summary.md`

### Initialize the database (first time only)

```bash
python schema/init_db.py
```

---

## 📊 Data

### Source

- **API**: [Open-Meteo](https://open-meteo.com/) — Free, no sign-up, no key required
- **Frequency**: Hourly updates, pipeline runs daily
- **Cities tracked**:

| City | Latitude | Longitude |
|------|----------|-----------| 
| New York, USA | 40.71 | -74.01 |
| London, UK | 51.51 | -0.13 |
| Tokyo, Japan | 35.68 | 139.69 |
| Sydney, Australia | -33.87 | 151.21 |
| Lagos, Nigeria | 6.52 | 3.38 |

### Metrics collected

- Temperature (°C and °F)
- Apparent temperature (feels like)
- Relative humidity (%)
- Wind speed (km/h) and direction (°)
- Precipitation (mm)
- Weather code (WMO standard)
- Derived: Heat Index, Wind Chill, Weather Category

---

## 🗄️ Database Schema

### `weather_observations` table

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment |
| city | TEXT | City name |
| latitude | REAL | Location latitude |
| longitude | REAL | Location longitude |
| observed_at | TEXT | ISO 8601 timestamp (UTC) |
| temp_c | REAL | Temperature in Celsius |
| temp_f | REAL | Temperature in Fahrenheit |
| feels_like_c | REAL | Apparent temperature |
| humidity_pct | REAL | Relative humidity % |
| wind_speed_kmh | REAL | Wind speed |
| wind_direction_deg | REAL | Wind direction in degrees |
| precipitation_mm | REAL | Precipitation |
| weather_code | INTEGER | WMO weather code |
| weather_category | TEXT | Derived: Clear/Cloudy/Rainy/Snowy/Stormy |
| heat_index_c | REAL | Derived heat index |
| wind_chill_c | REAL | Derived wind chill |
| ingested_at | TEXT | Pipeline run timestamp |

### `pipeline_runs` table

Tracks every pipeline execution with row counts and status.

---

## ⚙️ GitHub Actions

The pipeline runs automatically every day at **08:00 UTC** via `.github/workflows/daily_pipeline.yml`.

It:
1. Runs the full ETL pipeline
2. Commits the updated `weather.db` and `reports/daily_summary.md` back to the repo
3. On failure, the run is logged and visible in the Actions tab

---

## 🧪 Tests

```bash
pip install pytest
pytest tests/
```

---

## 🛠️ Tech Stack

| Tool | Purpose | Cost |
|------|---------|------|
| Open-Meteo API | Weather data source | Free, no key |
| Python 3.11 | ETL logic | Free |
| SQLite | Data warehouse | Free (built-in) |
| pandas | Data transformation | Free |
| requests | HTTP client | Free |
| GitHub Actions | Orchestration/scheduler | Free (2,000 min/month) |

---

## 📈 Sample Analytics Output

The daily report (`reports/daily_summary.md`) includes:

- Hottest and coldest city of the day
- Average temperature per city (7-day rolling)
- Total precipitation per city
- Pipeline run history and row counts

---

## 📝 License

MIT — free to use, fork, and build on.
