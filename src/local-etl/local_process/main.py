import subprocess


def run_data_processing():
    command = ["python", "data_processing.py"]
    subprocess.run(command)


if __name__ == "__main__":
    run_data_processing()
