import subprocess


def run_data_ingestion():
    command = ["python", "data_ingestion_local.py"]

    subprocess.run(command)


if __name__ == "__main__":
    run_data_ingestion()
