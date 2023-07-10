import subprocess


def run_data_ingestion():
    command = ["python", "./local-ingest/data_ingestion_local.py"]
    subprocess.run(command)


def run_data_processing():
    command = ["python", "./local-process/data_processing.py"]
    subprocess.run(command)


if __name__ == "__main__":
    run_data_ingestion()
    run_data_processing()
