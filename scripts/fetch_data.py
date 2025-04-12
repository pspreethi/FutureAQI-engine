import pandas as pd
import requests
import datetime
import os
from dotenv import load_dotenv
from pathlib import Path
from logging_util import setup_logger

# === Setup Logging ===
logger = setup_logger("fetch_data", "fetch_data.log")

# === Load Environment Variables ===
env_path = Path("config_template.env")
load_dotenv(dotenv_path=env_path)

# === Load API Key ===
API_KEY = os.getenv("OPENAQ_API_KEY")
if not API_KEY:
    logger.error("Missing OPENAQ_API_KEY in .env file.")
    raise ValueError("OPENAQ_API_KEY is not set. Please add it to your .env file.")

HEADERS = {"X-API-Key": API_KEY}
BASE_LOCATION_URL = "https://api.openaq.org/v3/locations"
BASE_SENSOR_URL = "https://api.openaq.org/v3/sensors"


# === Helper Functions ===
def fetch_paginated_data(url, params=None):
    page = 1
    limit = 1000
    all_results = []

    while True:
        try:
            full_url = f"{url}?page={page}&limit={limit}"
            logger.info(f"Fetching data from {full_url}")
            response = requests.get(full_url, headers=HEADERS, params=params)

            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])
                if not results:
                    break
                all_results.extend(results)
                logger.info(f"Fetched page {page} from {url}, total results: {len(results)}")
                page += 1
            else:
                logger.error(f"Failed to fetch data from {url} - Status Code: {response.status_code}")
                break
        except Exception as e:
            logger.error(f"Error fetching data from {url} - {e}")
            break

    logger.info(f"Completed fetching data from {url}, total records: {len(all_results)}")
    return all_results


def normalize_sensor_data(sensor_ids):
    all_sensor_data = []
    params = {
        "datetime_from": "2020-01-01",
        "datetime_to": "2025-02-25",
        "limit": 1000
    }

    for s_id in sensor_ids:
        try:
            logger.info(f"Fetching data for sensor ID: {s_id}")
            page = 1
            while True:
                params["page"] = page
                url = f"{BASE_SENSOR_URL}/{s_id}/measurements/daily"
                response = requests.get(url, headers=HEADERS, params=params)

                if response.status_code == 200:
                    data = response.json()
                    if not data.get("results"):
                        logger.info(f"No more data for sensor ID: {s_id} after page {page}")
                        break
                    for record in data["results"]:
                        record["sensor_id"] = s_id
                        all_sensor_data.append(record)
                    page += 1
                    logger.info(f"Fetched page {page} for sensor ID: {s_id}")
                else:
                    logger.error(f"Failed to fetch sensor {s_id} on page {page} - Status Code: {response.status_code}")
                    break
        except Exception as e:
            logger.error(f"Error fetching sensor {s_id}: {e}")

    logger.info(f"Completed fetching data for all sensors, total records: {len(all_sensor_data)}")
    return pd.json_normalize(all_sensor_data)


def main():
    try:
        logger.info("Starting data fetching process...")

        # Step 1: Fetch all location metadata
        all_locations = fetch_paginated_data(BASE_LOCATION_URL)

        # Step 2: Filter Indian, stationary, licensed PM2.5 sensors
        in_locations = [loc for loc in all_locations if loc.get("country", {}).get("code") == "IN"]
        logger.info(f"Total Indian locations found: {len(in_locations)}")

        df_all = pd.json_normalize(
            in_locations,
            record_path=["sensors"],
            meta=[
                "id", "name", "locality", "timezone", "isMobile", "isMonitor", "licenses", "instruments",
                ["bounds"], "distance", ["datetimeFirst", "utc"], ["datetimeLast", "utc"],
                ["country", "id"], ["country", "code"], ["country", "name"],
                ["owner", "id"], ["owner", "name"],
                ["provider", "id"], ["provider", "name"],
                ["coordinates", "latitude"], ["coordinates", "longitude"]
            ],
            record_prefix="s_",
            errors="ignore"
        )

        df_pm25 = df_all[df_all["s_name"] == "pm25 µg/m³"]
        current_year = datetime.datetime.now().year

        # Filter sensors for data duration and provider
        df_filtered = df_pm25[
            (df_pm25["provider.name"] == "AirNow") &
            (df_pm25["locality"].notna()) &
            (df_pm25["licenses"].notna())
        ]

        sensor_ids = list(df_filtered["s_id"])
        logger.info(f"Filtered sensor IDs: {sensor_ids}")

        # Save metadata
        df_filtered.to_csv("data/locations.csv", index=False)

        # Step 3: Fetch and save sensor data
        df_sensor_data = normalize_sensor_data(sensor_ids)
        if not df_sensor_data.empty:
            output_file = "data/openaq_combined_data.csv"
            os.makedirs("data", exist_ok=True)
            df_sensor_data.to_csv(output_file, index=False)
            logger.info(f"Data saved to {output_file}")
        else:
            logger.warning("No sensor data fetched.")
    except Exception as e:
        logger.error(f"Error in main function: {e}")


if __name__ == "__main__":
    main()
