from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """remove unwanted columns"""
    df = df.drop(columns=['HOOD_140', 'NEIGHBOURHOOD_140'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame) -> Path:
    """Write DataFrame out locally as csv file"""
    path = Path(f"data.csv")
    df.to_csv(path)
    return path

@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoomcampfinal")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""

    dataset_url = f"major-crime-indicators.csv"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean)
    write_gcs(path)




if __name__ == "__main__":
    etl_web_to_gcs()