from local_ingest.data_ingestion_local import data_ingestion
from local_process.data_processing import run_data_processing
from prefect import flow


def run_data_ingestion_step():
    data_ingestion()


def run_data_processing_step():
    run_data_processing()


@flow(name="ETL Flow", log_prints=True)
def main_flow():
    print("Step 1 Started")
    run_data_ingestion_step()
    print("Step 1 Ended")
    print("Step 2 Started")
    run_data_processing_step()
    print("Step 2 Ended")


if __name__ == "__main__":
    main_flow()
