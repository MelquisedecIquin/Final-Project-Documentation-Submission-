# 🌦️ Philippine Weather Streaming Dashboard

A big data pipeline that streams live weather readings from **WeatherAPI** through **Kafka** and **Spark Structured Streaming** into **MongoDB**, and shows them in a **Streamlit** dashboard with a live view and a full history view.

```
WeatherAPI ──► producer.py ──► Kafka ──┬──► Streamlit "Live" tab (consumes Kafka directly, refreshes every 15 s)
                                       │
                                       └──► Spark Structured Streaming ──► MongoDB ──► Streamlit "History" tab
                                            (raw + 1-minute aggregates)
```

| Component | File | What it does |
| :-- | :-- | :-- |
| Producer | `producer.py` | Calls WeatherAPI for 5 cities every 30 s and publishes one message per metric (temperature, humidity, pressure, wind) |
| Kafka | `docker-compose.yml` | Message broker (topic `weather-data`) |
| Spark | `spark_streaming.py` | Reads Kafka, writes raw readings to `weather.readings` and 1-minute windowed avg/min/max/count to `weather.aggregates_1min` |
| MongoDB | `docker-compose.yml` | Long-term storage |
| Dashboard | `app.py` | Live tab, History tab (filters, aggregation, all data), export to Excel / PDF / CSV |

## Message schema
`timestamp` (ISO 8601 UTC), `value` (number), `metric_type`, `sensor_id` (city), plus `location`, `unit`, `observed_at`.

## Setup (Windows)

**Prerequisites:** Python 3.10+, [Docker Desktop](https://www.docker.com/products/docker-desktop/), a free [WeatherAPI](https://www.weatherapi.com/) key.

```powershell
python -m venv .venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

copy .env.example .env      # then open .env and paste your WEATHERAPI_KEY
```

## Run (4 steps, in this order)

```powershell
# 1. Start Kafka, MongoDB and the Spark job (first run downloads images + Spark packages, ~2-5 min)
docker compose up -d

# 2. Start the producer (new terminal, venv activated)
python producer.py

# 3. Start the dashboard (another terminal, venv activated)
streamlit run app.py
```

Open http://localhost:8501. The Live tab fills within about 30 seconds. The History tab fills as soon as Spark has written its first batch.

Useful commands:

```powershell
docker compose logs -f spark      # watch Spark write batches to MongoDB
docker compose down               # stop everything (data is kept in Docker volumes)
docker compose down -v            # stop and DELETE all stored data
python producer.py --cities "Manila" "Cebu City" --interval 60
```

## Dashboard features
- **Live tab:** per-city KPI cards with change since last reading, multi-city line chart, data-freshness indicator, auto-refresh (5-60 s, default 15 s).
- **History tab:** time range (1h / 24h / 7d / 30d / All), metric and city filters, aggregation (raw / hourly / daily / weekly), summary statistics, Spark 1-minute aggregates, table of all collected data.
- **Export:** Excel (Data, Summary, Filters sheets), PDF report (summary table + charts), CSV.

## Notes
- Free WeatherAPI plan: about 1 million calls/month. 5 cities every 30 s is roughly 430,000 calls/month; raise `--interval` if you run it for long periods.
- `.env` holds your API key and is git-ignored. Never commit it.
