from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3,log_prints=True)
def extract_from_gcs(num : int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/All-delays-2018-19-P{num:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gas")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task(log_prints=True)
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-cred")

    df.to_gbq(
        destination_table="dezoomcamp.train_delay",
        project_id="data-engineering-camp-376113",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(num : int) -> None:
    """Main ETL flow to load data into Big Query"""
    # color = "green"
    # year = 2020
    # month = 1

    path = extract_from_gcs(num)
    write_bq(path)


@flow()
def etl_to_bq_parent_flow(
    nums: list[int] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
):
    # here
    for num in nums:
        etl_gcs_to_bq(num)


if __name__ == "__main__":
    nums = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
    etl_to_bq_parent_flow(nums)
