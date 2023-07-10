from prefect import task


def load(df_list, files_paths):
    for df, path in zip(df_list, files_paths):
        write_file(df, path)


@task(name="Data Load Task", log_prints=True)
def write_file(df, path):
    df.coalesce(1).write.parquet(path, mode="overwrite")


if __name__ == "__main__":
    load()
