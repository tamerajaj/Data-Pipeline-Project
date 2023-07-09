# A script that is directly uploading the data of the first 3 months of the
# Green data of the year 2021 to a GCS bucket. ######
import os

import click
import pandas as pd
from google.cloud import storage


@click.command()
@click.option("--sa_path", help="Path to the service account JSON file")
@click.option("--project_id", help="Project ID of your GCP project")
@click.option("--year", default=2021, help="Year to download")
@click.option("--bucket", help="Name of the bucket to upload the data")
@click.option("--months", default="1-3", help="Range of months to download (e.g., 1-3)")
@click.option(
    "--color",
    default="green",
    help="Color of taxi data to download (e.g., yellow, green)",
)
def data_ingestion(sa_path, project_id, year, bucket, months, color):
    try:
        start_month, end_month = map(int, months.split("-"))
    except ValueError:
        click.echo(
            "Invalid month range format. Please provide a valid range in the format "
            "'start-end'."
        )
        return

    if start_month < 1 or start_month > 12 or end_month < 1 or end_month > 12:
        click.echo("Invalid month values. Month values should be between 1 and 12.")
        return

    if end_month < start_month:
        click.echo(
            "Invalid month range. The second month should be larger or equal to the "
            "first month."
        )
        return

    for month in range(start_month, end_month + 1):
        url = (
            f"https://d37ci6vzurychx.cloudfront.net/trip-data/"
            f"{color}_tripdata_{year}-{month:02d}.parquet"
        )
        file_name = f"{color}_tripdata_{year}-{month:02d}.parquet"

        print("Loading data from URL...")
        df_taxi = pd.read_parquet(url)
        print("Uploading data to GCS...")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_path
        client = storage.Client()
        bucket = client.get_bucket(bucket)
        bucket.blob(f"{color}_taxi/{file_name}").upload_from_string(
            df_taxi.to_parquet(), "text/parquet"
        )
        print("Successfully uploaded the data!")


if __name__ == "__main__":
    data_ingestion()
