# Data Pipeline Project

Please do not fork this repository, but use this repository as a template for your refactoring project. Make Pull Requests to your own repository even if you work alone and mark the checkboxes with an x, if you are done with a topic in the pull request message.

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

## Quick guide:

### ETL pipelines:
1.  Local: In [./src/local-etl/etl_local_main.py](./src/local-etl/etl_local_main.py)
2.  Cloud: In [./src/cloud-etl/etl_cloud_main.py](./src/cloud-etl/etl_cloud_main.py)

### ELT pipelines:
In [.src/ELT](./src/ELT)

## Prefect:
Implemented for local ETL.
