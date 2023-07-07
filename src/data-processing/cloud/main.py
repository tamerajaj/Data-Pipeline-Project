import os
import subprocess

from dotenv import load_dotenv


def run_main_script():
    load_dotenv("../../../.env")

    bucket = os.getenv("BUCKET")
    project_id = os.getenv("PROJECT_ID")
    region = os.getenv("REGION")
    cluster_name = os.getenv("CLUSTER_NAME")
    dataset = os.getenv("DATASET")

    subprocess.run(
        [
            "python",
            "submit_job_to_cluster.py",
            "--project_id",
            project_id,
            "--region",
            region,
            "--cluster_name",
            cluster_name,
            "--gcs_bucket",
            bucket,
            "--output_dataset",
            dataset,
        ]
    )
    print("Successfully started the subprocess.")


if __name__ == "__main__":
    run_main_script()
