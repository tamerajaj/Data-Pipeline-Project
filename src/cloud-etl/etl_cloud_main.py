import os
import subprocess

from dotenv import load_dotenv


def run_data_ingestion():
    dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../.env")
    load_dotenv(dotenv_path)

    args = {
        "sa_path": os.environ.get("SA_PATH"),
        "bucket": os.environ.get("BUCKET"),
        "project_id": os.environ.get("PROJECT_ID"),
    }

    command = ["python", "./data-ingestion/data_ingestion.py"]

    for key, value in args.items():
        if value is not None:
            command.extend(["--" + key, str(value)])

    subprocess.run(command)


def run_main_script():
    load_dotenv("../../.env")

    bucket = os.getenv("BUCKET")
    project_id = os.getenv("PROJECT_ID")
    region = os.getenv("REGION")
    cluster_name = os.getenv("CLUSTER_NAME")
    dataset = os.getenv("DATASET")

    subprocess.run(
        [
            "python",
            "./data-processing/submit_job_to_cluster.py",
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
    run_data_ingestion()
    run_main_script()
