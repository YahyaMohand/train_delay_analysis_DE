from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from datetime import timedelta


@task(retries=3,log_prints=True, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    
    print(df)
    return df

@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gas")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    num = 1
    dataset_file = f"./train_delay/All-delays-2018-19-P{num:02}.csv"
    dataset_url = f"https://sacuksprodnrdigital0001.blob.core.windows.net/historic-delay-attribution/2018-19/All-Delays-2018-19-P{num:02}.zip"
    df = fetch(dataset_url)
    print(df.columns)
    path = write_local(df, dataset_file)
    write_gcs(path)



if __name__ == "__main__":
    etl_web_to_gcs()
