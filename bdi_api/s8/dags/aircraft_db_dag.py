import os
import logging
from datetime import datetime, timedelta
import requests
import gzip
import json
import pandas as pd
from io import BytesIO

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
AIRCRAFT_DB_URL = "http://downloads.adsbexchange.com/downloads/basic-ac-db.json.gz"
# Use your default bucket name
S3_BUCKET = Variable.get("s3_bucket", default_var="bdi-s8-data-lake-5103")
AWS_CONN_ID = "aws_default"
# Ensure this connection is configured in Airflow
POSTGRES_CONN_ID = "postgres_default"
RDS_TARGET_TABLE = "aircraft"  # Target table in RDS
SOURCE_FILENAME = "basic-ac-db.json.gz"
SILVER_FILENAME = "aircraft_database.parquet"

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),  # Adjust start date if needed
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def download_db_to_bronze(**context):
    """
    Downloads the aircraft database gzipped JSON file and uploads it to the S3 bronze layer.
    Checks for existence first to ensure idempotency.
    """
    logical_date = context['ds']
    year, month, day = logical_date.split('-')
    s3_key = f"bronze/aircraft-db/{year}/{month}/{day}/{SOURCE_FILENAME}"
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    logger.info(f"Checking for existing file: s3://{S3_BUCKET}/{s3_key}")

    # Idempotency Check
    if s3_hook.check_for_key(s3_key, bucket_name=S3_BUCKET):
        logger.info(
            f"File {s3_key} already exists in S3 bronze layer. Skipping download.")
        # Optionally, push the key to XComs if needed by downstream tasks
        context['ti'].xcom_push(key='bronze_s3_key', value=s3_key)
        return s3_key
    else:
        logger.info(f"File not found. Downloading from {AIRCRAFT_DB_URL}")
        try:
            response = requests.get(AIRCRAFT_DB_URL, stream=True, timeout=30)
            response.raise_for_status()  # Raise an exception for bad status codes

            # Upload the gzipped content directly to S3
            s3_hook.load_file_obj(
                # Use BytesIO to treat content as file-like object
                file_obj=BytesIO(response.content),
                key=s3_key,
                bucket_name=S3_BUCKET,
                replace=True  # Replace if somehow check failed but file exists partially
            )
            logger.info(
                f"Successfully downloaded and uploaded to s3://{S3_BUCKET}/{s3_key}")
            # Push the key to XComs
            context['ti'].xcom_push(key='bronze_s3_key', value=s3_key)
            return s3_key

        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading file: {e}")
            raise
        except Exception as e:
            logger.error(f"Error uploading file to S3: {e}")
            raise


def process_bronze_to_silver(**context):
    """
    Reads the gzipped JSON Lines file from bronze, processes each line into a structured Parquet format,
    and uploads it to the S3 silver layer.
    """
    ti = context['ti']
    bronze_s3_key = ti.xcom_pull(
        task_ids='download_db_to_bronze', key='bronze_s3_key')

    if not bronze_s3_key:
        logger.error("Bronze S3 key not found in XComs. Cannot proceed.")
        raise ValueError("Bronze S3 key missing from XComs")

    logger.info(f"Processing file: s3://{S3_BUCKET}/{bronze_s3_key}")
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    records = []
    try:
        # Read the gzipped file from S3
        s3_object_response = s3_hook.get_conn().get_object(
            Bucket=S3_BUCKET, Key=bronze_s3_key)
        compressed_content = s3_object_response.get('Body').read()

        # Decompress and read line by line
        with gzip.GzipFile(fileobj=BytesIO(compressed_content), mode='rb') as gz_file:
            line_number = 0
            for line_bytes in gz_file:
                line_number += 1
                try:
                    # Decode line and parse as JSON
                    line_str = line_bytes.decode('utf-8').strip()
                    if not line_str:  # Skip empty lines
                        continue
                    details = json.loads(line_str)

                    # Extract fields directly from the line's JSON object
                    icao = details.get('icao', '')
                    record = {
                        # Remove potential leading '~'
                        'icao': icao.strip('~') if icao else '',
                        # Changed key from 'r' to 'reg' based on log
                        'registration': details.get('reg'),
                        # Changed key from 't' to 'icaotype' based on log
                        'type': details.get('icaotype'),
                        # Changed key from 'mfr' based on log
                        'manufacturer': details.get('manufacturer'),
                        # Changed key from 'mdl' based on log
                        'model': details.get('model'),
                        # Changed key from 'op' based on log
                        'operator': details.get('ownop'),
                        # Changed key from 'own' based on log
                        'owner': details.get('owner')
                        # Add other fields if needed based on actual JSON structure
                    }
                    # Ensure icao is not empty before appending
                    if record['icao']:
                        records.append(record)
                    else:
                        logger.warning(
                            f"Skipping record on line {line_number} due to missing or empty ICAO.")

                except json.JSONDecodeError as e:
                    logger.warning(
                        f"Skipping invalid JSON on line {line_number}: {e}. Line content: {line_bytes[:100]}...")
                    continue  # Skip this line and continue with the next
                except Exception as e:
                    logger.error(f"Error processing line {line_number}: {e}")
                    # Depending on requirements, you might want to fail the task here or just log and continue
                    # For now, log and continue to process other lines
                    continue

    except FileNotFoundError:
        logger.error(f"Bronze file {bronze_s3_key} not found in S3.")
        raise
    except gzip.BadGzipFile:
        logger.error(f"File {bronze_s3_key} is not a valid Gzip file.")
        raise
    except Exception as e:
        logger.error(
            f"Error during S3 read or decompression for {bronze_s3_key}: {e}")
        raise

    # --- Continue processing with the collected 'records' list ---
    if not records:
        logger.warning(
            f"No valid aircraft records were parsed from {bronze_s3_key}. No silver file created.")
        return None  # Indicate no silver file was created

    try:
        df = pd.DataFrame(records)
        logger.info(f"Created DataFrame with {len(df)} records.")

        # Data Cleaning (basic example - can be expanded)
        df = df.fillna('')  # Replace NaN with empty strings for consistency

        # Determine Silver S3 key (use the same date path as bronze)
        bronze_path_parts = bronze_s3_key.split('/')
        # bronze/aircraft-db/YYYY/MM/DD/SOURCE_FILENAME -> silver/aircraft-db/YYYY/MM/DD/SILVER_FILENAME
        silver_s3_key = f"silver/{bronze_path_parts[1]}/{bronze_path_parts[2]}/{bronze_path_parts[3]}/{bronze_path_parts[4]}/{SILVER_FILENAME}"

        logger.info(
            f"Uploading processed data to s3://{S3_BUCKET}/{silver_s3_key}")

        # Convert DataFrame to Parquet in memory
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        # Upload Parquet file to S3 Silver
        s3_hook.load_file_obj(
            file_obj=parquet_buffer,
            key=silver_s3_key,
            bucket_name=S3_BUCKET,
            replace=True
        )
        logger.info(f"Successfully uploaded Parquet file to S3 silver layer.")

        # Push silver key to XComs
        ti.xcom_push(key='silver_s3_key', value=silver_s3_key)
        return silver_s3_key

    except Exception as e:
        logger.error(f"Error during DataFrame processing or S3 upload: {e}")
        raise


def load_silver_to_rds(**context):
    """
    Reads the processed Parquet data from S3 Silver and upserts it into the target RDS table.
    """
    ti = context['ti']
    silver_s3_key = ti.xcom_pull(
        task_ids='process_bronze_to_silver', key='silver_s3_key')

    if not silver_s3_key:
        logger.warning("Silver S3 key not found in XComs. Skipping RDS load.")
        # This might happen if process_bronze_to_silver found no valid records
        return

    logger.info(
        f"Loading data from s3://{S3_BUCKET}/{silver_s3_key} to RDS table {RDS_TARGET_TABLE}")
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # Get SQLAlchemy engine for to_sql
    try:
        engine = pg_hook.get_sqlalchemy_engine()
        logger.info(
            f"Successfully obtained SQLAlchemy engine for {POSTGRES_CONN_ID}")
    except Exception as e:
        logger.error(f"Failed to get SQLAlchemy engine: {e}")
        raise

    # Define a unique temporary table name
    temp_table_name = f"temp_{RDS_TARGET_TABLE}_{os.urandom(4).hex()}"
    logger.info(f"Using temporary table name: {temp_table_name}")

    try:
        # Read Parquet file from S3 into pandas DataFrame
        s3_object_response = s3_hook.get_conn().get_object(
            Bucket=S3_BUCKET, Key=silver_s3_key)
        parquet_content = s3_object_response.get('Body').read()
        df = pd.read_parquet(BytesIO(parquet_content))

        if df.empty:
            logger.info("DataFrame is empty. No data to load into RDS.")
            return

        logger.info(f"Read {len(df)} records from Parquet file.")

        # Define columns that ACTUALLY exist in the target 'aircraft' table
        target_columns = ['icao', 'registration', 'type']

        # Select only the columns that exist in BOTH the DataFrame AND the target table
        available_columns = [
            col for col in target_columns if col in df.columns]
        if not available_columns or 'icao' not in available_columns:
            logger.warning(
                f"Required columns (incl. 'icao') not found in DataFrame {df.columns}. Skipping load.")
            return

        logger.info(
            f"Selecting columns {available_columns} for temporary table {temp_table_name}.")
        df_to_load = df[available_columns].copy()

        # Handle NaNs before to_sql
        df_to_load = df_to_load.where(pd.notnull(df_to_load), None)

        # 1. Load data into the temporary table using the fast to_sql method
        logger.info(
            f"Loading {len(df_to_load)} rows into temporary table {temp_table_name}...")
        df_to_load.to_sql(name=temp_table_name, con=engine, if_exists='replace',
                          index=False, method='multi', chunksize=10000)  # Adjust chunksize if needed
        logger.info(
            f"Successfully loaded data into temporary table {temp_table_name}.")

        # 2. Construct and execute the bulk UPSERT statement
        quoted_target_columns = [f'\"{col}\"' for col in available_columns]
        quoted_conflict_target = '\"icao\"'  # Primary key for aircraft table
        # Construct SET statements, excluding the conflict target itself
        update_set_statements = ", ".join(
            [f'\"{col}\" = EXCLUDED.\"{col}\"' for col in available_columns if col != 'icao'])

        # Ensure there are columns to update, otherwise, only insert non-conflicting rows
        if update_set_statements:
            upsert_sql = f"""
            INSERT INTO \"{RDS_TARGET_TABLE}\" ({', '.join(quoted_target_columns)})
            SELECT {', '.join(quoted_target_columns)} FROM \"{temp_table_name}\"
            ON CONFLICT ({quoted_conflict_target}) DO UPDATE SET
              {update_set_statements};
            """
        else:
            # If only the PK column ('icao') is selected, only INSERT new rows
            upsert_sql = f"""
            INSERT INTO \"{RDS_TARGET_TABLE}\" ({', '.join(quoted_target_columns)})
            SELECT {', '.join(quoted_target_columns)} FROM \"{temp_table_name}\"
            ON CONFLICT ({quoted_conflict_target}) DO NOTHING;
            """

        logger.info(
            f"Executing UPSERT/INSERT from temporary table {temp_table_name} to target table {RDS_TARGET_TABLE}...")
        logger.debug(f"UPSERT SQL: {upsert_sql}")
        pg_hook.run(upsert_sql, autocommit=True)
        logger.info(
            f"Successfully upserted/inserted data from {temp_table_name} to {RDS_TARGET_TABLE}.")

    except FileNotFoundError:
        logger.error(f"Silver file {silver_s3_key} not found in S3.")
        raise
    except Exception as e:
        logger.error(
            f"Error during temporary table load or upsert for {RDS_TARGET_TABLE}: {e}")
        raise
    finally:
        # 3. Drop the temporary table
        try:
            logger.info(f"Dropping temporary table {temp_table_name}...")
            with pg_hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(f'DROP TABLE IF EXISTS \"{temp_table_name}\";')
            logger.info(
                f"Successfully dropped temporary table {temp_table_name}.")
        except Exception as drop_err:
            logger.warning(
                f"Could not drop temporary table {temp_table_name}: {drop_err}")


# Define the DAG
with DAG(
    dag_id='aircraft_database_pipeline',
    default_args=default_args,
    description='Downloads and processes the Aircraft Database.',
    schedule_interval='@monthly',  # Or set to None for manual runs
    catchup=False,
    tags=['s8', 'aircraft', 'database'],
) as dag:

    # Define tasks
    download_task = PythonOperator(
        task_id='download_db_to_bronze',
        python_callable=download_db_to_bronze,
        provide_context=True,
    )

    process_task = PythonOperator(
        task_id='process_bronze_to_silver',
        python_callable=process_bronze_to_silver,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_silver_to_rds',
        python_callable=load_silver_to_rds,
        provide_context=True,
    )

    # Define dependencies
    download_task >> process_task >> load_task
