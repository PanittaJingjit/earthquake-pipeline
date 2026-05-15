import pandas as pd
import json
import glob
import os

RAW_PATH = "/opt/airflow/data/raw"
PROCESSED_PATH = "/opt/airflow/data/processed"


def extract_location(place):

    if pd.isna(place):
        return "Unknown", "Unknown"

    parts = place.split(",")

    if len(parts) >= 2:

        country = parts[-1].strip()

        location = ",".join(parts[:-1]).strip()

        return location, country

    return place, "Unknown"


def clean_data():

    os.makedirs(PROCESSED_PATH, exist_ok=True)

    files = glob.glob(
        f"{RAW_PATH}/earthquakes_*.json"
    )

    if not files:
        raise Exception("No raw files found")

    latest_file = max(files)

    print(f"Reading file: {latest_file}")

    with open(latest_file) as f:
        data = json.load(f)

    rows = []

    for feature in data["features"]:

        props = feature.get("properties", {})
        geometry = feature.get("geometry", {})

        coords = geometry.get(
            "coordinates",
            [None, None, None]
        )

        location, country = extract_location(
            props.get("place")
        )

        rows.append({

            "earthquake_id": feature.get("id"),

            "magnitude": props.get("mag"),

            "place": props.get("place"),

            "location": location,

            "country": country,

            "event_time": props.get("time"),

            "longitude": coords[0],

            "latitude": coords[1],

            "depth": coords[2]
        })

    df = pd.DataFrame(rows)

    print(f"Initial rows: {len(df)}")

    # Remove duplicates
    df.drop_duplicates(
        subset=["earthquake_id"],
        inplace=True
    )

    # Numeric conversion
    numeric_cols = [
        "magnitude",
        "longitude",
        "latitude",
        "depth"
    ]

    for col in numeric_cols:

        df[col] = pd.to_numeric(
            df[col],
            errors="coerce"
        )

    # Timestamp conversion
    df["event_time"] = pd.to_datetime(
        df["event_time"],
        unit="ms",
        errors="coerce"
    )

    # Remove missing
    df.dropna(
        subset=[
            "earthquake_id",
            "magnitude",
            "latitude",
            "longitude"
        ],
        inplace=True
    )

    # Coordinate validation
    df = df[
        (df["latitude"].between(-90, 90))
        &
        (df["longitude"].between(-180, 180))
    ]

    # Magnitude validation
    df = df[
        (df["magnitude"] >= -2)
        &
        (df["magnitude"] <= 10)
    ]

    # Depth validation
    df = df[df["depth"] >= 0]

    # Fill missing
    df["place"] = df["place"].fillna(
        "Unknown"
    )

    df["location"] = df["location"].fillna(
        "Unknown"
    )

    df["country"] = df["country"].fillna(
        "Unknown"
    )

    # Add ingestion timestamp
    df["ingestion_time"] = pd.Timestamp.now()

    print(f"Cleaned rows: {len(df)}")

    timestamp = pd.Timestamp.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    output_path = (
        f"{PROCESSED_PATH}/"
        f"cleaned_earthquakes_{timestamp}.csv"
    )

    df.to_csv(output_path, index=False)

    print(f"Cleaned data saved: {output_path}")


if __name__ == "__main__":
    clean_data()