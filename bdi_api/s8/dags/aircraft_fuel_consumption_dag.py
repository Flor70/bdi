from __future__ import annotations

import pendulum
import logging
import requests
import json
import io
import pandas as pd
import os
from typing import Optional

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException

# Constants
S3_CONN_ID = "aws_default"  # Assumes default AWS connection
# Assumes an Airflow Connection named 'postgres_rds' is configured
# POSTGRES_CONN_ID = "postgres_rds"
POSTGRES_CONN_ID = "postgres_default"  # Use the verified default connection
S3_BUCKET_NAME = "bdi-s8-data-lake-5103"  # From Terraform output
FUEL_DATA_URL = "https://github.com/martsec/flight_co2_analysis/raw/main/data/aircraft_type_fuel_consumption_rates.json"
FUEL_DATA_TABLE_NAME = "fuel_consumption"  # Align with plan.md
FUEL_CONSUMPTION_URL = "https://raw.githubusercontent.com/junzis/aircraft-db/master/data/aircraft-type-fuel-consumption-rates.json"
BRONZE_PREFIX = "bronze/fuel-consumption"
FILENAME = "aircraft-type-fuel-consumption-rates.json"
SILVER_PREFIX = "silver/fuel-consumption"

logger = logging.getLogger(__name__)


@task
def download_fuel_data_to_bronze(bucket_name: str, bronze_prefix: str, filename: str, url: str, s3_conn_id: str, logical_date: pendulum.DateTime) -> Optional[str]:
    """(Task) Downloads fuel consumption JSON and uploads to S3 bronze layer with date partitioning, checking for existence first."""
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)

    # Create date-specific path: bronze/fuel-consumption/YYYY-MM-DD/filename.json
    date_str = logical_date.to_date_string()
    s3_key = f"{bronze_prefix}/{date_str}/{filename}"
    logger.info(f"Target S3 Key: s3://{bucket_name}/{s3_key}")

    # 1. Check if file already exists for this logical date (Idempotency)
    try:
        if s3_hook.check_for_key(key=s3_key, bucket_name=bucket_name):
            logger.info(
                f"File already exists for date {date_str} at {s3_key}. Skipping download.")
            # Return the key so downstream tasks know it exists, even if skipped
            return s3_key
        else:
            logger.info(
                f"File does not exist for date {date_str}. Proceeding with download.")
    except Exception as e:
        logger.error(f"Error checking S3 for key {s3_key}: {e}")
        raise  # Fail the task if we can't check S3

    # 2. Download step
    logger.info(f"Attempting download from: {url}")
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        file_content = response.content  # Get content as bytes

        # 3. Upload step
        logger.debug(f"Initiating S3 upload to {s3_key}")
        s3_hook.load_bytes(bytes_data=file_content,
                           key=s3_key,
                           bucket_name=bucket_name,
                           replace=True)  # Replace shouldn't matter due to check above, but safe default
        logger.info(
            f"Successfully downloaded and uploaded {filename} to s3://{bucket_name}/{s3_key}")
        return s3_key

    except requests.exceptions.Timeout as e:
        logger.error(f"Timeout error downloading {url}: {e}")
        raise
    except requests.exceptions.HTTPError as e:
        logger.error(
            f"HTTP error downloading {url}: Status {e.response.status_code} - {e.response.reason}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"General request error downloading {url}: {e}")
        raise
    except Exception as e:
        logger.error(
            f"Error during upload of {filename} to S3 key {s3_key}: {e}", exc_info=True)
        raise


@dag(
    dag_id="aircraft_fuel_consumption_pipeline",
    start_date=pendulum.datetime(
        2024, 7, 30, tz="UTC"),  # Align with readsb DAG
    schedule=None,  # Manual trigger for now, or align with others if needed
    catchup=False,
    tags=["bdi", "s8", "fuel_consumption"],
    doc_md="""
    ### Aircraft Fuel Consumption Data Pipeline

    This DAG downloads the aircraft fuel consumption JSON file from GitHub,
    stores it in the S3 bronze layer, processes it into the silver layer (Parquet),
    and loads the result into the corresponding RDS table.

    **Steps:**
    1. Downloads the source JSON file if it doesn't exist for the logical date in S3 bronze.
    2. Processes the bronze JSON into a structured Parquet file in S3 silver.
    3. Loads the silver Parquet data into the `fuel_consumption` table in RDS.

    **Connections Required:**
    - `aws_default`: AWS Connection for S3 access.
    - `postgres_default`: Postgres Connection for RDS access.
    """,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=2),
    }
)
def aircraft_fuel_consumption_pipeline():

    # Task 4.1: Download source JSON to Bronze Layer
    bronze_s3_key_output = download_fuel_data_to_bronze(
        bucket_name=S3_BUCKET_NAME,
        bronze_prefix=BRONZE_PREFIX,
        filename=FILENAME,
        url=FUEL_DATA_URL,
        s3_conn_id=S3_CONN_ID
        # logical_date is automatically injected
    )

    @task
    def process_fuel_to_silver(bronze_s3_key: Optional[str], bucket_name: str, silver_prefix: str, s3_conn_id: str) -> Optional[str]:
        """(Task 4.2) Transforms fuel JSON from bronze to Parquet in silver layer."""
        if not bronze_s3_key:
            logger.warning(
                "No bronze S3 key provided, skipping silver processing.")
            raise AirflowSkipException(
                "Upstream task skipped or returned None.")

        s3_hook = S3Hook(aws_conn_id=s3_conn_id)
        logger.info(
            f"Processing bronze file: s3://{bucket_name}/{bronze_s3_key}")

        # Infer Silver key path from Bronze key path
        path_parts = bronze_s3_key.split('/')
        if len(path_parts) >= 4:
            # Assumes bronze_key like bronze/fuel-consumption/YYYY-MM-DD/filename.json
            date_str = path_parts[-2]  # YYYY-MM-DD
            base_filename = path_parts[-1].replace('.json', '.parquet')
            silver_s3_key = f"{silver_prefix}/{date_str}/{base_filename}"
            logger.info(
                f"Target Silver S3 Key: s3://{bucket_name}/{silver_s3_key}")
        else:
            logger.error(
                f"Could not infer date path from bronze key: {bronze_s3_key}")
            raise ValueError(f"Invalid bronze key format: {bronze_s3_key}")

        try:
            # 1. Read raw JSON content from S3 Bronze
            raw_data_bytes = s3_hook.read_key(
                key=bronze_s3_key, bucket_name=bucket_name)
            data = json.loads(raw_data_bytes)

            # 2. Parse and Transform
            # Assuming the JSON is a dictionary where keys are aircraft types
            # and values are dictionaries containing 'galph' (gallons-per-hour)
            records = []
            for aircraft_type, details in data.items():
                if isinstance(details, dict) and 'galph' in details:
                    records.append({
                        # Use snake_case matching plan.md FuelConsumption model
                        "aircraft_type": aircraft_type,
                        "galph": details.get('galph'),
                        # Include other fields if available
                        "name": details.get('name'),
                        "category": details.get('category'),
                        "source": details.get('source')
                    })
                else:
                    logger.warning(
                        f"Skipping unexpected format for key: {aircraft_type} in {bronze_s3_key}")

            if not records:
                logger.warning(
                    f"No valid fuel consumption records found in {bronze_s3_key}. Skipping silver write.")
                raise AirflowSkipException(
                    f"No records parsed from {bronze_s3_key}")

            df = pd.DataFrame(records)
            # Add basic cleaning/type casting if needed
            df['galph'] = pd.to_numeric(df['galph'], errors='coerce')
            # Drop rows where galph couldn't be parsed
            df = df.dropna(subset=['galph'])
            logger.info(f"Processed into DataFrame with shape: {df.shape}")

            if df.empty:
                logger.warning(
                    f"DataFrame empty after cleaning for {bronze_s3_key}. Skipping silver write.")
                raise AirflowSkipException(
                    f"DataFrame empty after cleaning {bronze_s3_key}")

            # 3. Write DataFrame to Parquet in memory
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)

            # 4. Upload Parquet file to S3 silver layer
            logger.info(
                f"Writing silver file to s3://{bucket_name}/{silver_s3_key}")
            s3_hook.load_file_obj(
                file_obj=parquet_buffer, key=silver_s3_key, bucket_name=bucket_name, replace=True)

            logger.info(
                f"Successfully processed {bronze_s3_key} to {silver_s3_key}")
            return silver_s3_key

        except json.JSONDecodeError as json_err:
            logger.error(
                f"Failed to decode JSON from {bronze_s3_key}: {json_err}")
            raise
        except Exception as e:
            logger.error(
                f"Error processing file {bronze_s3_key} to silver: {e}", exc_info=True)
            raise  # Fail the task

    # --- Add Task 4.2 (Transform Bronze to Silver) here ---
    silver_s3_key_output = process_fuel_to_silver(
        bronze_s3_key=bronze_s3_key_output,  # Pass output from previous task
        bucket_name=S3_BUCKET_NAME,
        silver_prefix=SILVER_PREFIX,
        s3_conn_id=S3_CONN_ID
    )

    @task
    def load_fuel_to_rds(silver_s3_key: Optional[str], bucket_name: str, table_name: str, s3_conn_id: str, postgres_conn_id: str):
        """(Task 4.3) Loads fuel consumption data from Silver Parquet into RDS."""
        if not silver_s3_key:
            logger.warning("No silver S3 key provided, skipping RDS load.")
            raise AirflowSkipException(
                "Upstream task skipped or returned None.")

        s3_hook = S3Hook(aws_conn_id=s3_conn_id)
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

        try:
            logger.info(
                f"Reading silver data for RDS load: s3://{bucket_name}/{silver_s3_key}")
            file_obj = s3_hook.get_key(
                key=silver_s3_key, bucket_name=bucket_name)
            silver_df = pd.read_parquet(
                io.BytesIO(file_obj.get()['Body'].read()))

            if silver_df.empty:
                logger.warning(
                    f"DataFrame from {silver_s3_key} is empty, skipping load to table {table_name}.")
                raise AirflowSkipException(
                    f"Input DataFrame empty {silver_s3_key}")

            # Define target columns based on plan.md FuelConsumption model
            target_columns = ['aircraft_type',
                              'name', 'galph', 'category', 'source']
            available_target_columns = [
                col for col in target_columns if col in silver_df.columns]

            if 'aircraft_type' not in available_target_columns:
                logger.error(
                    f"Primary key 'aircraft_type' not found in DataFrame columns: {silver_df.columns}. Cannot load to RDS.")
                raise ValueError(
                    "Missing primary key 'aircraft_type' for RDS load")

            logger.info(
                f"Selecting final columns for RDS table '{table_name}': {available_target_columns}")
            rds_df = silver_df[available_target_columns].copy()

            # Convert DataFrame records to list of tuples for insert_rows
            rds_df = rds_df.replace({pd.NaT: None})
            rds_df = rds_df.where(pd.notnull(rds_df), None)
            rows_to_insert = list(rds_df.itertuples(index=False, name=None))

            if not rows_to_insert:
                logger.warning(
                    f"No rows to insert into {table_name} after conversion.")
                raise AirflowSkipException(
                    "No rows to insert after conversion.")

            logger.info(
                f"Loading {len(rows_to_insert)} rows from {silver_s3_key} into RDS table: {table_name}")

            pg_hook.insert_rows(
                table=table_name,
                rows=rows_to_insert,
                target_fields=available_target_columns,
                replace=True,
                # Primary key for fuel_consumption
                replace_index=['aircraft_type']
            )
            logger.info(
                f"Successfully inserted/updated data into {table_name}.")

        except Exception as e:
            logger.error(
                f"Error reading silver file {silver_s3_key} or loading to RDS table {table_name}: {e}", exc_info=True)
            raise

    # --- Add Task 4.3 (Load Silver to RDS) here ---
    load_fuel_to_rds(
        silver_s3_key=silver_s3_key_output,  # Pass output from previous task
        bucket_name=S3_BUCKET_NAME,
        table_name=FUEL_DATA_TABLE_NAME,
        s3_conn_id=S3_CONN_ID,
        postgres_conn_id=POSTGRES_CONN_ID  # Use the standard postgres connection
    )


# Instantiate the DAG
aircraft_fuel_consumption_pipeline()
