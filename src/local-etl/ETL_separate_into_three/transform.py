import os

from prefect import task
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def transform(input_paths, year=2021, months="1-3", color="green"):
    spark = SparkSession.builder.appName(
        f"{color.capitalize()} Taxi {year}"
    ).getOrCreate()

    try:
        start_month, end_month = map(int, months.split("-"))
    except ValueError:
        print(
            "Invalid month range format. Please provide a valid range in the "
            "format 'start-end'."
        )
        return

    if start_month < 1 or start_month > 12 or end_month < 1 or end_month > 12:
        print("Invalid month values. Month values should be between 1 and 12.")
        return

    if end_month < start_month:
        print(
            "Invalid month range. The second month should be larger or equal "
            "to the first month."
        )
        return
    df_list = []
    files_paths = []
    for month, input_path in zip(range(start_month, end_month + 1), input_paths):
        df, file_path = data_processing_per_month(input_path, color, month, spark, year)
        df_list.append(df)
        files_paths.append(file_path)
    return df_list, files_paths


@task(name="Data Transform Task", log_prints=True)
def data_processing_per_month(input_path, color, month, spark, year):
    current_directory = os.path.dirname(os.path.abspath(__file__))

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
    return df_result, output_path


if __name__ == "__main__":
    transform()
