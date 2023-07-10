import os

import click
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@click.command()
@click.option("--year", default=2021, help="Year to download")
@click.option("--months", default="1-3", help="Range of months to download (e.g., 1-3)")
@click.option(
    "--color",
    default="green",
    help="Color of taxi data to download (e.g., yellow, green)",
)
def run_data_processing(year, months, color):
    spark = SparkSession.builder.appName(
        f"{color.capitalize()} Taxi {year}"
    ).getOrCreate()

    try:
        start_month, end_month = map(int, months.split("-"))
    except ValueError:
        click.echo(
            "Invalid month range format. Please provide a valid range in the "
            "format 'start-end'."
        )
        return

    if start_month < 1 or start_month > 12 or end_month < 1 or end_month > 12:
        click.echo("Invalid month values. Month values should be between 1 and 12.")
        return

    if end_month < start_month:
        click.echo(
            "Invalid month range. The second month should be larger or equal "
            "to the first month."
        )
        return

    for month in range(start_month, end_month + 1):
        current_directory = os.path.dirname(os.path.abspath(__file__))

        input_path = os.path.join(
            current_directory, f"../data/{color}_tripdata_{year}-{month:02d}.parquet/"
        )
        output_path = os.path.join(
            current_directory,
            f"../data/output/{color}_tripdata_{year}-{month:02d}_daily_revenue",
        )

        df = spark.read.parquet(input_path)
        df = df.filter(
            (F.month("lpep_dropoff_datetime") == month)
            & (F.year("lpep_dropoff_datetime") == year)
        )
        df = df.withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")

        revenue_day_col = F.date_format(F.col("dropoff_datetime"), "yyyy-MM-dd").alias(
            "dropoff_date"
        )

        df_result = (
            df.groupBy(revenue_day_col)
            .agg(
                F.format_number(F.sum("total_amount"), 2).alias(
                    "revenue_daily_total_amount"
                )
            )
            .orderBy("dropoff_date")
        )

        df_result.coalesce(1).write.parquet(output_path, mode="overwrite")


if __name__ == "__main__":
    run_data_processing()
