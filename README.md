# Data Pipeline Project


## Project for today
The task for today you can find in the [data-pipeline-project.md](data-pipeline-project.md) file.

## Setup:
```bash
pyenv local 3.11.3
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
```
## Questions:
### 1. What are the steps you took to complete the project?
* ETL:
  * ETL local: Data Extraction, Transforming and loading on local *.parquet files.
  * ETL cloud:
    * Extraction: Downloading dataset locally then uploading it to GCS bucket.
    * Transforming: Transforming data on Dataproc using a PySpark job.
    * Loading: Loading data from temporary GCS bucket to BigQuery.
* ETL:
  * Using DBT cloud and bigquery.
* Orchestrating:
  * Using Prefect for local ETL.

### 2. What are the challenges you faced?
* Submitting PySpark jobs to Dataproc was failing due to cluster issues: had to restart.
* DBT cloud does not allow for downloading the repo, I had to copy it by hand.
* Passing a list of sources to DBT model.
* Prefect: I had to refactor the code to make separate functions for the `@task` decorator.
### 3. What are the things you would do differently if you had more time?
* Add more data cleaning steps from week 1.
* Decide on one pipeline from the beginning and building a modular code for it.
* Refactor the Extraction step: At the moment there is a mix between using the click library and passing variables by
hand. It would be nice to be able to choose the color, year, month range when calling the ETL or ELT pipeline.
## Quick guide:

### ETL pipelines:
1.  Local: In [./src/local-etl](./src/local-etl)
2.  Cloud: In [./src/cloud-etl](./src/cloud-etl)

### ELT pipelines:
In [.src/ELT](./src/ELT)
