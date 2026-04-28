"""Weather/AQI Kafka producer.

Two modes:
  --mode simulated   → generate synthetic weather events (fast, no API calls)
  --mode open-meteo  → fetch real conditions from Open-Meteo's free APIs
                       (geocode each city once, cached to disk)

Each batch picks N random locations from the manifest, builds an event per
location, and publishes JSON to the Kafka topic (default `environment_raw`).
"""

import argparse
import csv
import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

from kafka import KafkaProducer


def parse_args():
    parser = argparse.ArgumentParser(
        description="Produce simulated weather events to Kafka."
    )
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="environment_raw")
    parser.add_argument(
        "--locations-dir",
        required=True,
        help="Directory containing CSV location manifest part files.",
    )
    parser.add_argument(
        "--mode",
        choices=("simulated", "open-meteo"),
        default="simulated",
        help="Whether to emit simulated data or fetch real data from Open-Meteo.",
    )
    parser.add_argument("--messages-per-batch", type=int, default=25)
    parser.add_argument("--interval-seconds", type=float, default=5.0)
    parser.add_argument(
        "--iterations",
        type=int,
        default=0,
        help="Number of batches to send. Use 0 to run forever.",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and print events without sending them to Kafka.",
    )
    parser.add_argument(
        "--min-patient-count",
        type=int,
        default=1,
        help="Ignore manifest rows with fewer than this many patients.",
    )
    parser.add_argument(
        "--country-code",
        default="US",
        help="Country code used for Open-Meteo geocoding lookups.",
    )
    parser.add_argument(
        "--geocode-cache-path",
        default="/mnt/synthea_data/cache/open_meteo_geocode_cache.json",
        help="JSON file used to cache geocoding lookups for real API mode.",
    )
    parser.add_argument(
        "--request-timeout-seconds",
        type=float,
        default=20.0,
        help="HTTP timeout for Open-Meteo API calls.",
    )
    return parser.parse_args()


def load_locations(locations_dir, min_patient_count):
    records = []
    for path in sorted(Path(locations_dir).glob("*.csv")):
        with path.open(newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                patient_count = int(row.get("patient_count") or 0)
                if row.get("location_id") and patient_count >= min_patient_count:
                    row["patient_count"] = patient_count
                    records.append(row)

    if not records:
        raise ValueError(f"No CSV location records found under {locations_dir}")

    return records


def simulate_weather(location, rng):
    # Gentle seasonal-ish simulation tuned for a respiratory risk demo.
    temperature_c = round(rng.uniform(-5, 34), 1)
    pm25 = round(max(0.0, rng.gauss(18, 12)), 1)
    pm10 = round(max(0.0, rng.gauss(30, 18)), 1)
    ozone = round(max(0.0, rng.gauss(70, 28)), 1)
    nitrogen_dioxide = round(max(0.0, rng.gauss(24, 12)), 1)
    humidity = round(min(100.0, max(20.0, rng.gauss(65, 18))), 1)
    wind_speed = round(max(0.0, rng.gauss(5, 2.5)), 1)
    aqi = int(min(300, max(0, pm25 * 2.2 + rng.uniform(0, 20))))

    return {
        "event_id": f"{location['location_id']}-{int(time.time() * 1000)}-{rng.randint(1000, 9999)}",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "location_id": location["location_id"],
        "city": location.get("city") or None,
        "state": location.get("state") or None,
        "zip": location.get("zip") or None,
        "lat": float(location["lat"]) if location.get("lat") else None,
        "lon": float(location["lon"]) if location.get("lon") else None,
        "temperature_c": temperature_c,
        "pm25": pm25,
        "pm10": pm10,
        "ozone": ozone,
        "nitrogen_dioxide": nitrogen_dioxide,
        "aqi": aqi,
        "humidity": humidity,
        "wind_speed": wind_speed,
        "source": "simulated",
    }


def load_geocode_cache(cache_path):
    path = Path(cache_path)
    if not path.exists():
        return {}

    with path.open() as handle:
        return json.load(handle)


def save_geocode_cache(cache_path, cache):
    path = Path(cache_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as handle:
        json.dump(cache, handle, indent=2, sort_keys=True)


def fetch_json(base_url, params, timeout_seconds):
    url = f"{base_url}?{urlencode(params)}"
    with urlopen(url, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def geocode_query(location):
    zip_code = (location.get("zip") or "").strip()
    city = (location.get("city") or "").strip()
    state = (location.get("state") or "").strip()

    if zip_code:
        return zip_code

    if city and state:
        return f"{city}, {state}"

    return city or location["location_id"]


def pick_geocode_result(results, location):
    if not results:
        return None

    zip_code = (location.get("zip") or "").strip()
    if zip_code:
        for result in results:
            if zip_code in (result.get("postcodes") or []):
                return result

    return results[0]


def geocode_location(location, cache, args):
    cache_key = location["location_id"]
    cached = cache.get(cache_key)
    if cached:
        return cached

    payload = fetch_json(
        "https://geocoding-api.open-meteo.com/v1/search",
        {
            "name": geocode_query(location),
            "count": 10,
            "language": "en",
            "countryCode": args.country_code,
            "format": "json",
        },
        args.request_timeout_seconds,
    )

    result = pick_geocode_result(payload.get("results") or [], location)
    if result is None:
        raise ValueError(f"Could not geocode location {location['location_id']}")

    resolved = {
        "latitude": result["latitude"],
        "longitude": result["longitude"],
        "timezone": result.get("timezone") or "UTC",
        "resolved_name": result.get("name"),
    }
    cache[cache_key] = resolved
    save_geocode_cache(args.geocode_cache_path, cache)
    return resolved


def fetch_open_meteo_event(location, cache, args):
    geocoded = geocode_location(location, cache, args)
    latitude = geocoded["latitude"]
    longitude = geocoded["longitude"]

    weather_payload = fetch_json(
        "https://api.open-meteo.com/v1/forecast",
        {
            "latitude": latitude,
            "longitude": longitude,
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
            "timezone": "auto",
            "forecast_days": 1,
        },
        args.request_timeout_seconds,
    )
    air_payload = fetch_json(
        "https://air-quality-api.open-meteo.com/v1/air-quality",
        {
            "latitude": latitude,
            "longitude": longitude,
            "current": "pm2_5,pm10,ozone,nitrogen_dioxide,us_aqi",
            "timezone": "auto",
            "domains": "auto",
        },
        args.request_timeout_seconds,
    )

    weather_current = weather_payload.get("current") or {}
    air_current = air_payload.get("current") or {}

    local_time_str = weather_current.get("time") or air_current.get("time")
    if local_time_str:
        from zoneinfo import ZoneInfo
        local_dt = datetime.fromisoformat(local_time_str).replace(
            tzinfo=ZoneInfo(geocoded.get("timezone") or "UTC")
        )
        event_time = local_dt.astimezone(timezone.utc).isoformat()
    else:
        event_time = datetime.now(timezone.utc).isoformat()

    return {
        "event_id": f"{location['location_id']}-{int(time.time() * 1000)}",
        "event_time": event_time,
        "location_id": location["location_id"],
        "city": location.get("city") or None,
        "state": location.get("state") or None,
        "zip": location.get("zip") or None,
        "lat": latitude,
        "lon": longitude,
        "temperature_c": weather_current.get("temperature_2m"),
        "pm25": air_current.get("pm2_5"),
        "pm10": air_current.get("pm10"),
        "ozone": air_current.get("ozone"),
        "nitrogen_dioxide": air_current.get("nitrogen_dioxide"),
        "aqi": air_current.get("us_aqi"),
        "humidity": weather_current.get("relative_humidity_2m"),
        "wind_speed": weather_current.get("wind_speed_10m"),
        "source": "open-meteo",
    }


def main():
    args = parse_args()
    rng = random.Random(args.seed)
    locations = load_locations(args.locations_dir, args.min_patient_count)
    geocode_cache = load_geocode_cache(args.geocode_cache_path)

    producer = None
    if not args.dry_run:
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            key_serializer=lambda key: key.encode("utf-8"),
        )

    iteration = 0
    while args.iterations == 0 or iteration < args.iterations:
        batch = rng.sample(locations, k=min(args.messages_per_batch, len(locations)))
        sent_count = 0
        for location in batch:
            try:
                if args.mode == "open-meteo":
                    event = fetch_open_meteo_event(location, geocode_cache, args)
                else:
                    event = simulate_weather(location, rng)
            except Exception as exc:
                print(f"Skipping {location['location_id']}: {exc}")
                continue

            if args.dry_run:
                print(json.dumps(event, sort_keys=True))
            else:
                producer.send(args.topic, key=event["location_id"], value=event)
            sent_count += 1

        if producer is not None:
            producer.flush()
        iteration += 1
        if args.dry_run:
            print(
                f"Previewed batch {iteration} with {sent_count} weather events "
                f"using {args.mode}"
            )
        else:
            print(
                f"Sent batch {iteration} with {sent_count} weather events to {args.topic} "
                f"on {args.bootstrap_servers} using {args.mode}"
            )

        if args.iterations == 0 or iteration < args.iterations:
            time.sleep(args.interval_seconds)


if __name__ == "__main__":
    main()
