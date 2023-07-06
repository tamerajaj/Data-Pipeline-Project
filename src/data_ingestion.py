import os

import click
import pandas as pd
from google.cloud import storage


@click.command()
@click.option("--sa_path", help="Path to the service account json file")
@click.option("--project_id", help="Project ID of you GCP project")
@click.option("--year", default=2021, help="Year to download")
@click.option("--bucket", help="Name of the bucket to upload the data")
def data_ingestion(sa_path, project_id, year, bucket):
    color = "yellow"
    for month in range(1, 4):
        url = (
            f"https://d37ci6vzurychx.cloudfront.net/trip-data/"
            f"{color}_tripdata_{year}-{month:02d}.parquet"
        )

        file_name = f"{color}_tripdata_{year}-{month:02d}.parquet"

        print("Loading data from url...")
        df_taxi = pd.read_parquet(url)
        print("Uploading data to GCS...")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_path
        client = storage.Client()
        bucket = client.get_bucket(bucket)
        bucket.blob(f"yellow_taxi/{file_name}").upload_from_string(
            df_taxi.to_parquet(), "text/parquet"
        )
        print("Successfully uploaded the data!")

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_path
        client = storage.Client()
        bucket = client.get_bucket(bucket)
        bucket.blob(f"yellow_taxi/{file_name}").upload_from_string(
            df_taxi.to_parquet(), "text/parquet"
        )


if __name__ == "__main__":
    data_ingestion()
