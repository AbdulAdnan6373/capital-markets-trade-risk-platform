from datetime import date
from pathlib import Path

import boto3


# AWS configuration
AWS_PROFILE = "trade-risk"
AWS_REGION = "us-east-1"
S3_BUCKET = "cm-trade-risk-lake-adnan-6373"


# Find the main project directory
PROJECT_ROOT = Path(__file__).resolve().parents[2]


# Datasets that we want to upload
DATASETS = [
    "accounts",
    "securities",
    "counterparties",
    "trades",
    "market_prices",
    "positions",
]


# Connect to AWS using the configured CLI profile
session = boto3.Session(
    profile_name=AWS_PROFILE,
    region_name=AWS_REGION,
)

s3_client = session.client("s3")


# Today's date will become part of the S3 folder
ingestion_date = date.today().isoformat()


for dataset_name in DATASETS:
    local_file = (
        PROJECT_ROOT
        / "data"
        / "generated"
        / dataset_name
        / f"{dataset_name}.parquet"
    )

    s3_key = (
        f"raw/{dataset_name}/"
        f"ingestion_date={ingestion_date}/"
        f"{dataset_name}.parquet"
    )

    if not local_file.exists():
        print(f"Skipped: file does not exist: {local_file}")
        continue

    print(f"Uploading {dataset_name}...")

    s3_client.upload_file(
        str(local_file),
        S3_BUCKET,
        s3_key,
    )

    print(f"Uploaded to s3://{S3_BUCKET}/{s3_key}")


print("Raw data ingestion completed.")