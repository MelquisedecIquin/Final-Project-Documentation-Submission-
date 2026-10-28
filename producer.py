"""
Kafka Producer: real weather data from WeatherAPI (https://www.weatherapi.com/)

Pipeline position:  WeatherAPI  ->  [this producer]  ->  Kafka  ->  Spark  ->  MongoDB  ->  Streamlit

Every cycle the producer calls the WeatherAPI "current" endpoint once per city and
publishes one Kafka message per metric (temperature, humidity, pressure, wind).

Message schema (JSON):
{
    "timestamp":   "2026-10-28T00:35:10Z",  # ISO 8601, UTC, time the reading was fetched
    "value":       31.2,                    # numeric measurement
    "metric_type": "temperature",           # temperature | humidity | pressure | wind
    "sensor_id":   "Quezon City",           # city the reading belongs to
    "location":    "Quezon City, Philippines",
    "unit":        "celsius",
    "observed_at": "2026-10-28 08:30"       # local time reported by WeatherAPI
}
"""

import argparse
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

load_dotenv()

API_URL = "https://api.weatherapi.com/v1/current.json"

DEFAULT_CITIES = ["Quezon City", "Cebu City", "Davao City", "Baguio", "Iloilo City"]

# metric_type -> (field in WeatherAPI "current" object, unit)
METRICS = {
    "temperature": ("temp_c", "celsius"),
    "humidity": ("humidity", "percent"),
    "pressure": ("pressure_mb", "hPa"),
    "wind": ("wind_kph", "kph"),
}


class StreamingDataProducer:
    """Fetches weather data from WeatherAPI and sends it to a Kafka topic."""

    def __init__(self, bootstrap_servers: str, topic: str, api_key: str, cities: List[str]):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.api_key = api_key
        self.cities = cities
        self.session = requests.Session()
        self.producer: Optional[KafkaProducer] = None

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                acks="all",
                retries=3,
            )
            print(f"Kafka producer initialized for {bootstrap_servers} on topic '{topic}'")
        except NoBrokersAvailable:
            print(f"ERROR: No Kafka brokers available at {bootstrap_servers}. Is Docker running?")
        except Exception as e:
            print(f"ERROR: Failed to initialize Kafka producer: {e}")

    # ------------------------------------------------------------------ API
    def fetch_city(self, city: str, retries: int = 3) -> Optional[Dict[str, Any]]:
        """Call WeatherAPI for one city. Returns the parsed JSON or None on failure."""
        params = {"key": self.api_key, "q": city, "aqi": "no"}
        for attempt in range(1, retries + 1):
            try:
                resp = self.session.get(API_URL, params=params, timeout=10)
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code in (401, 403):
                    print(f"ERROR: API key rejected ({resp.status_code}). Check WEATHERAPI_KEY in .env")
                    return None
                if resp.status_code == 400:
                    print(f"ERROR: WeatherAPI could not find '{city}': {resp.text[:120]}")
                    return None
                print(f"WARN: API returned {resp.status_code} for {city} (attempt {attempt}/{retries})")
            except requests.RequestException as e:
                print(f"WARN: API request failed for {city} (attempt {attempt}/{retries}): {e}")
            time.sleep(2 * attempt)
        return None

    @staticmethod
    def build_records(city: str, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Turn one WeatherAPI response into one record per metric."""
        try:
            current = payload["current"]
            loc = payload["location"]
        except (KeyError, TypeError):
            return []

        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        location = ", ".join(p for p in (loc.get("name"), loc.get("country")) if p)
        records = []
        for metric, (field, unit) in METRICS.items():
            value = current.get(field)
            if value is None:
                continue
            records.append(
                {
                    "timestamp": now,
                    "value": float(value),
                    "metric_type": metric,
                    "sensor_id": city,
                    "location": location,
                    "unit": unit,
                    "observed_at": current.get("last_updated"),
                }
            )
        return records

    def fetch_weather_records(self) -> List[Dict[str, Any]]:
        """Fetch all cities and return a flat list of records."""
        records: List[Dict[str, Any]] = []
        for city in self.cities:
            payload = self.fetch_city(city)
            if payload:
                records.extend(self.build_records(city, payload))
        return records

    # ---------------------------------------------------------------- Kafka
    @staticmethod
    def serialize_data(data: Dict[str, Any]) -> Optional[bytes]:
        try:
            return json.dumps(data).encode("utf-8")
        except (TypeError, ValueError) as e:
            print(f"ERROR: Serialization failed: {e}")
            return None

    def send_message(self, data: Dict[str, Any]) -> bool:
        if not self.producer:
            print("ERROR: Kafka producer not initialized")
            return False
        payload = self.serialize_data(data)
        if payload is None:
            return False
        try:
            future = self.producer.send(
                self.topic, key=data["sensor_id"].encode("utf-8"), value=payload
            )
            future.get(timeout=10)
            return True
        except KafkaError as e:
            print(f"Kafka send error: {e}")
        except Exception as e:
            print(f"Unexpected error during send: {e}")
        return False

    def produce_stream(self, interval: float = 30.0, duration: Optional[int] = None):
        """Fetch + publish every `interval` seconds (until `duration` seconds elapse, if set)."""
        print(f"Starting producer: cities={self.cities}, every {interval}s, duration={duration or 'infinite'}")
        start = time.time()
        total_sent = 0
        try:
            while True:
                if duration and (time.time() - start) >= duration:
                    print(f"Reached duration limit of {duration} seconds")
                    break

                cycle_start = time.time()
                records = self.fetch_weather_records()
                sent = sum(1 for r in records if self.send_message(r))
                total_sent += sent
                stamp = datetime.now().strftime("%H:%M:%S")
                print(f"[{stamp}] fetched {len(records)} records, sent {sent} (total sent: {total_sent})")

                time.sleep(max(0.0, interval - (time.time() - cycle_start)))
        except KeyboardInterrupt:
            print("\nProducer interrupted by user")
        finally:
            self.close()
            print(f"Producer stopped. Total messages sent: {total_sent}")

    def close(self):
        if self.producer:
            try:
                self.producer.flush(timeout=10)
                self.producer.close()
                print("Kafka producer closed successfully")
            except Exception as e:
                print(f"Error closing Kafka producer: {e}")


def parse_arguments():
    parser = argparse.ArgumentParser(description="WeatherAPI -> Kafka streaming producer")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BROKER", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "weather-data"))
    parser.add_argument(
        "--interval", type=float, default=30.0,
        help="Seconds between API fetch cycles (default 30; free plan allows ~1M calls/month)",
    )
    parser.add_argument("--duration", type=int, default=None, help="Run time in seconds (default: infinite)")
    parser.add_argument(
        "--cities", nargs="+", default=DEFAULT_CITIES,
        help='Cities to track, e.g. --cities "Quezon City" Cebu Davao',
    )
    return parser.parse_args()


def main():
    print("=" * 60)
    print("WEATHER STREAMING PRODUCER (WeatherAPI -> Kafka)")
    print("=" * 60)

    args = parse_arguments()

    api_key = os.getenv("WEATHERAPI_KEY")
    if not api_key:
        print("ERROR: WEATHERAPI_KEY is not set. Copy .env.example to .env and add your key.")
        return

    producer = StreamingDataProducer(args.bootstrap_servers, args.topic, api_key, args.cities)
    if not producer.producer:
        return
    producer.produce_stream(interval=args.interval, duration=args.duration)


if __name__ == "__main__":
    main()
