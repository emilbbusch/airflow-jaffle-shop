from typing import Any
from airflow.sdk import asset, task
from azure.storage.blob import BlobServiceClient
from azure.core.credentials import AzureSasCredential
import pandas as pd
import duckdb
import io
import os

duckdb_database_path = os.environ["DUCKDB_DATABASE_PATH"]
azure_storage_account_name = os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
azure_storage_sas_token = os.environ["AZURE_STORAGE_SAS_TOKEN"]

container_name = "jaffle-shop"

def read_blob_from_adls2(blob_service_client, blob_name: str) -> pd.DataFrame:
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=blob_name
    )
    csv_bytes = blob_client.download_blob().readall()
    return pd.read_csv(io.BytesIO(csv_bytes))


def load_data(table_name: str, blob_name: str, blob_service_client: BlobServiceClient):
    df = read_blob_from_adls2(blob_service_client, blob_name)
    with duckdb.connect(duckdb_database_path) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        conn.execute(f"DROP TABLE IF EXISTS raw.{table_name}")
        conn.execute(f"CREATE TABLE raw.{table_name} AS SELECT * FROM df")


blob_service_client = BlobServiceClient(
    account_url=f"{azure_storage_account_name}.dfs.core.windows.net",
    credential=AzureSasCredential(azure_storage_sas_token),
)

