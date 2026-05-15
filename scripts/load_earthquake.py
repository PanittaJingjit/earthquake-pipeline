import pandas as pd
import glob

from sqlalchemy import (
    create_engine,
    text
)

DB_URL = (
    "postgresql+psycopg2://"
    "airflow:airflow@postgres/airflow"
)

PROCESSED_PATH = (
    "/opt/airflow/data/processed"
)


def load_data():

    files = glob.glob(
        f"{PROCESSED_PATH}/"
        f"transformed_earthquakes_*.csv"
    )

    if not files:
        raise Exception(
            "No transformed files found"
        )

    latest_file = max(files)

    print(f"Loading file: {latest_file}")

    df = pd.read_csv(latest_file)

    engine = create_engine(DB_URL)

    with engine.begin() as conn:

        conn.execute(text("""

            CREATE SCHEMA IF NOT EXISTS silver;

        """))

        conn.execute(text("""

            CREATE TABLE IF NOT EXISTS
            silver.cleaned_earthquakes (

                earthquake_id TEXT PRIMARY KEY,

                magnitude FLOAT,

                place TEXT,

                location TEXT,

                country TEXT,

                event_time TIMESTAMP,

                longitude FLOAT,

                latitude FLOAT,

                depth FLOAT,

                ingestion_time TIMESTAMP,

                severity TEXT,

                depth_category TEXT,

                magnitude_category TEXT,

                risk_score FLOAT,

                year INT,

                month INT,

                day INT,

                hour INT,

                weekday TEXT,

                hemisphere TEXT,

                time_of_day TEXT
            );

        """))

        for _, row in df.iterrows():

            query = text("""

                INSERT INTO
                silver.cleaned_earthquakes (

                    earthquake_id,
                    magnitude,
                    place,
                    location,
                    country,
                    event_time,
                    longitude,
                    latitude,
                    depth,
                    ingestion_time,
                    severity,
                    depth_category,
                    magnitude_category,
                    risk_score,
                    year,
                    month,
                    day,
                    hour,
                    weekday,
                    hemisphere,
                    time_of_day

                )

                VALUES (

                    :earthquake_id,
                    :magnitude,
                    :place,
                    :location,
                    :country,
                    :event_time,
                    :longitude,
                    :latitude,
                    :depth,
                    :ingestion_time,
                    :severity,
                    :depth_category,
                    :magnitude_category,
                    :risk_score,
                    :year,
                    :month,
                    :day,
                    :hour,
                    :weekday,
                    :hemisphere,
                    :time_of_day

                )

                ON CONFLICT (earthquake_id)
                DO NOTHING

            """)

            conn.execute(
                query,
                row.to_dict()
            )

    print("Data loaded successfully")


if __name__ == "__main__":
    load_data()