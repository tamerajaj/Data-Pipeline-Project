from extract import extract
from load import load
from prefect import flow
from transform import transform


@flow(name="ETL Flow", log_prints=True)
def main_flow():
    print("Step 1 Started")
    input_paths = extract()
    print("Step 1 Ended")
    print("Step 2 Started")
    df_list, output_paths = transform(input_paths)
    print("Step 2 Ended")
    print("Step 2 Started")
    load(df_list, output_paths)
    print("Step 2 Ended")


if __name__ == "__main__":
    main_flow()
