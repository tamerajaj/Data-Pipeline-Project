import argparse

import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def main(
    project_id: str,
    year: int,
    bucket: str,
    months: str,
    color: str,
    output_dataset: str,
):
    spark = SparkSession.builder.appName("GreenTaxiDailyRevenue").getOrCreate()
    spark.conf.set("temporaryGcsBucket", bucket)

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
    for month in range(start_month, end_month + 1):
        input_path = (
            f"gs://{bucket}/{color}_taxi/{color}_tripdata_{year}-{month:02d}.parquet/"
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
                ),
            )
            .orderBy("dropoff_date")
        )

        df_result.write.format("bigquery").option(
            "table",
            f"{project_id}.{output_dataset}.{color}_{year}_{month}_daily_revenue",
        ).save(mode="overwrite")
        print("Successfully uploaded the data!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", help="Project ID of your GCP project")
    parser.add_argument("--year", default=2021, type=int, help="Year to process")
    parser.add_argument("--bucket", help="Name of the bucket to upload the data")
    parser.add_argument(
        "--months", default="1-3", help="Range of months to process (e.g., 1-3)"
    )
    parser.add_argument(
        "--color",
        default="green",
        help="Color of taxi data to process (e.g., yellow, green)",
    )
    parser.add_argument("--output_dataset", help="BigQuery output dataset name")
    args = parser.parse_args()

    main(
        args.project_id,
        args.year,
        args.bucket,
        args.months,
        args.color,
        args.output_dataset,
    )
