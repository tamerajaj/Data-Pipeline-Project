#!/usr/bin/env python

import argparse
import os
import re

from google.cloud import dataproc_v1, storage


def upload_local_file_to_bucket(project, bucket_name, source_file, destination_file):
    print(f"Uploading {source_file} to Cloud Storage.")
    client = storage.Client(project=project)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(destination_file)

    current_directory = os.path.dirname(os.path.abspath(__file__))
    source_file_path = os.path.join(current_directory, source_file)
    blob.upload_from_filename(source_file_path)


def submit_job_to_cluster(
    project_id,
    region,
    cluster_name,
    gcs_bucket,
    pyspark_file,
    year,
    months,
    color,
    output_dataset,
):
    upload_local_file_to_bucket(project_id, gcs_bucket, pyspark_file, f"{pyspark_file}")

    job_client = dataproc_v1.JobControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    job = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{gcs_bucket}/code/{pyspark_file}",
            "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"],
            "args": [
                f"--year={year}",
                f"--months={months}",
                f"--color={color}",
                f"--output_dataset={output_dataset}",
                f"--bucket={gcs_bucket}",
                f"--project_id={project_id}",
            ],
        },
    }

    operation = job_client.submit_job_as_operation(
        request={"project_id": project_id, "region": region, "job": job}
    )
    response = operation.result()
    print(response)

    matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)

    output = (
        storage.Client(project_id)
        .get_bucket(matches.group(1))
        .blob(f"{matches.group(2)}.000000000")
        .download_as_bytes()
        .decode("utf-8")
    )

    print(f"Job finished successfully: {output}\r\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--project_id", type=str, required=True)
    parser.add_argument("--region", type=str, required=True)
    parser.add_argument("--cluster_name", type=str, required=True)
    parser.add_argument("--gcs_bucket", type=str, required=True)
    parser.add_argument("--pyspark_file", type=str, default="data_processing.py")
    parser.add_argument("--year", default=2021, type=int)
    parser.add_argument("--months", default="1-3", type=str)
    parser.add_argument("--color", default="green", type=str)
    parser.add_argument("--output_dataset", type=str, required=True)
    args = parser.parse_args()
    submit_job_to_cluster(
        args.project_id,
        args.region,
        args.cluster_name,
        args.gcs_bucket,
        args.pyspark_file,
        args.year,
        args.months,
        args.color,
        args.output_dataset,
    )
