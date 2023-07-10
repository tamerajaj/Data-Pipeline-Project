import os

import pandas as pd
from prefect import task


def extract(year=2021, months="1-3", color="green"):
    try:
        start_month, end_month = map(int, months.split("-"))
    except ValueError:
        print(
            "Invalid month range format. Please provide a valid range in the "
            "format 'start-end'."
        )
        return

    if start_month < 1 or start_month > 12 or end_month < 1 or end_month > 12:
        print("Invalid month values. Month values should be between 1 " "and 12.")
        return

    if end_month < start_month:
        print(
            "Invalid month range. The second month should be larger or equal "
            "to the first month."
        )
        return
    paths = []
    for month in range(start_month, end_month + 1):
        path = data_ingest_by_month(color, month, year)
        paths.append(path)
    return paths


@task(name="Data Extract Task - per month", log_prints=True)
def data_ingest_by_month(color, month, year):
    url = (
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/"
        f"{color}_tripdata_{year}-{month:02d}.parquet"
    )
    file_name = f"{color}_tripdata_{year}-{month:02d}.parquet"
    print("Loading data from URL...")
    df_taxi = pd.read_parquet(url)
    print("Saving data locally...")
    current_directory = os.path.dirname(os.path.abspath(__file__))
    source_file_path = os.path.join(current_directory, f"../data/{file_name}")
    df_taxi.to_parquet(source_file_path, index=False)
    print("Successfully saved the data!")
    return source_file_path


if __name__ == "__main__":
    extract()
