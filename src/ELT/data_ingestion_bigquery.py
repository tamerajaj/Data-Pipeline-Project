# import click
import os

import pandas as pd
from dotenv import load_dotenv
from google.oauth2 import service_account


# TODO: use click to call function from a main
# @click.command()
# @click.option('--sa_path', help='Path to the service account json file')
# @click.option('--project_id', help='Project ID of you GCP project')
# @click.option('--month', default=1, help='Month of the year to download')
# @click.option('--year', default=2021, help='The year to download')
# @click.option('--color', default='green', help='The color to download')
def data_ingestion_bigquery(sa_path, project_id, month, year, color):
    url = (
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{year}"
        f"-{month:02d}.parquet"
    )

    print("Loading data from url...")
    df_taxi = pd.read_parquet(url)

    credentials = service_account.Credentials.from_service_account_file(
        sa_path,
    )

    table_name = f"{color}_taxi_raw_merged.merged_tripdata_{year}"

    print("Uploading data to GBQ...")
    df_taxi.to_gbq(
        table_name,
        project_id=project_id,
        credentials=credentials,
        chunksize=10000,
        progress_bar=True,
        if_exists="append",
    )

    print("Successfully uploaded the data!")


if __name__ == "__main__":
    dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../.env")
    load_dotenv(dotenv_path)
    print(os.environ.get("SA_PATH_BGQ"))
    for pick_month in [1, 2, 3]:  # TODO: refactor month call
        data_ingestion_bigquery(
            sa_path=os.environ.get("SA_PATH_BGQ"),
            project_id=os.environ.get("PROJECT_ID"),
            month=pick_month,
            year=2021,
            color="green",
        )
