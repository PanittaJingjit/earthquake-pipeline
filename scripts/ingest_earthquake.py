import requests
import json
import os
from datetime import datetime

URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"

RAW_PATH = "/opt/airflow/data/raw"


def ingest_data():

    os.makedirs(RAW_PATH, exist_ok=True)

    print("Fetching earthquake data from API...")

    response = requests.get(URL)

    if response.status_code != 200:
        raise Exception(
            f"API request failed: {response.status_code}"
        )

    data = response.json()

    timestamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    output_path = (
        f"{RAW_PATH}/"
        f"earthquakes_{timestamp}.json"
    )

    with open(output_path, "w") as f:
        json.dump(data, f)

    print(f"Raw data saved: {output_path}")


if __name__ == "__main__":
    ingest_data()