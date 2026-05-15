import pandas as pd
import glob

PROCESSED_PATH = "/opt/airflow/data/processed"


def classify_severity(mag):

    if mag >= 5:
        return "High"

    elif mag >= 3:
        return "Moderate"

    return "Low"


def classify_depth(depth):

    if depth < 70:
        return "Shallow"

    elif depth < 300:
        return "Intermediate"

    return "Deep"


def magnitude_category(mag):

    if mag < 2:
        return "Micro"

    elif mag < 4:
        return "Minor"

    elif mag < 6:
        return "Moderate"

    return "Major"


def calculate_risk_score(row):

    return round(
        (row["magnitude"] * 2)
        + (row["depth"] / 10),
        2
    )


def transform_data():

    files = glob.glob(
        f"{PROCESSED_PATH}/cleaned_earthquakes_*.csv"
    )

    print(f"Found files: {files}")

    if not files:
        raise Exception(
            "No processed files found"
        )

    latest_file = max(files)

    print(
        f"Transforming file: {latest_file}"
    )

    df = pd.read_csv(latest_file)

    # Severity
    df["severity"] = df[
        "magnitude"
    ].apply(classify_severity)

    # Depth category
    df["depth_category"] = df[
        "depth"
    ].apply(classify_depth)

    # Magnitude category
    df["magnitude_category"] = df[
        "magnitude"
    ].apply(magnitude_category)

    # Risk score
    df["risk_score"] = df.apply(
        calculate_risk_score,
        axis=1
    )

    # Time features
    df["event_time"] = pd.to_datetime(
        df["event_time"]
    )

    df["year"] = (
        df["event_time"].dt.year
    )

    df["month"] = (
        df["event_time"].dt.month
    )

    df["day"] = (
        df["event_time"].dt.day
    )

    df["hour"] = (
        df["event_time"].dt.hour
    )

    df["weekday"] = (
        df["event_time"].dt.day_name()
    )

    # Hemisphere
    df["hemisphere"] = df[
        "latitude"
    ].apply(
        lambda x:
        "Northern"
        if x >= 0
        else "Southern"
    )

    # Day/Night
    df["time_of_day"] = df[
        "hour"
    ].apply(
        lambda x:
        "Night"
        if x < 6
        else "Day"
    )

    timestamp = pd.Timestamp.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    output_path = (
        f"{PROCESSED_PATH}/"
        f"transformed_earthquakes_{timestamp}.csv"
    )

    df.to_csv(output_path, index=False)

    print(
        f"Transformed data saved: {output_path}"
    )


if __name__ == "__main__":
    transform_data()