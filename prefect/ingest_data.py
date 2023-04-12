from pathlib import Path
import pandas as pd
import requests
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
import ssl
ssl._create_default_https_context = ssl._create_unverified_context


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    crime_df = pd.read_csv(dataset_url)
    return crime_df


@task(log_prints=True)
def clean(crime_df: pd.DataFrame) -> pd.DataFrame:
    """remove unwanted columns"""
    crime_df = crime_df.drop(columns=['HOOD_140', 'NEIGHBOURHOOD_140'])
    print(crime_df.head(2))
    print(f"columns: {crime_df.dtypes}")
    print(f"rows: {len(crime_df)}")
    return crime_df


# @task()
# def write_local(dataset_url: str) -> Path:
#     """Write DataFrame out locally as csv file"""
#     path = Path(f"data.csv")
#     df.to_csv(path)
#     return path

@task()
def write_gcs(crime_df: pd.DataFrame) -> None:
    """Upload local parquet file to GCS"""
    gcs_bucket = GcsBucket.load("zoomcampfinal")
    gcs_bucket.upload_from_dataframe(df=crime_df,to_path="data.csv", serialization_format='csv')
    return


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
 
    # Datasets are called "packages". Each package can contain many "resources"
    # To retrieve the metadata for this package and its resources, use the package name in this page's URL:
    url = base_url + "/api/3/action/package_show"
    params = { "id": "major-crime-indicators"}
    package = requests.get(url, params = params).json()
    temp_url = package['result']['resources'][3]['url']

    dataset_url = temp_url

    df = fetch(dataset_url)
    df_clean = clean(df)
    write_gcs(df_clean)




if __name__ == "__main__":
    etl_web_to_gcs()
