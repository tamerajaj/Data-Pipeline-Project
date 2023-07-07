import os
import subprocess

from dotenv import load_dotenv


def run_data_ingestion():
    dotenv_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "../../../.env"
    )
    load_dotenv(dotenv_path)

    args = {
        "sa_path": os.environ.get("SA_PATH"),
        "bucket": os.environ.get("BUCKET"),
        "project_id": os.environ.get("PROJECT_ID"),
    }

    command = ["python", "data_ingestion.py"]

    for key, value in args.items():
        if value is not None:
            command.extend(["--" + key, str(value)])

    subprocess.run(command)


if __name__ == "__main__":
    run_data_ingestion()
