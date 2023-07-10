import os

import click
import pandas as pd


@click.command()
@click.option("--year", default=2021, help="Year to download")
@click.option("--months", default="1-3", help="Range of months to download (e.g., 1-3)")
@click.option(
    "--color",
    default="green",
    help="Color of taxi data to download (e.g., yellow, green)",
)
def data_ingestion(year, months, color):
    try:
        start_month, end_month = map(int, months.split("-"))
    except ValueError:
        click.echo(
            "Invalid month range format. Please provide a valid range in the "
            "format 'start-end'."
        )
        return

    if start_month < 1 or start_month > 12 or end_month < 1 or end_month > 12:
        click.echo("Invalid month values. Month values should be between 1 " "and 12.")
        return

    if end_month < start_month:
        click.echo(
            "Invalid month range. The second month should be larger or equal "
            "to the first month."
        )
        return

    for month in range(start_month, end_month + 1):
        data_ingest_by_month(color, month, year)


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


if __name__ == "__main__":
    data_ingestion()
