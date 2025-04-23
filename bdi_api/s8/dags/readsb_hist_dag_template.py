"""
DAG for downloading and processing readsb-hist data from ADS-B Exchange.
This template provides the structure for the DAG, but you will need to implement
the actual functions for downloading, processing, and loading the data.
"""

import os
import json
import gzip
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
import boto3
from io import BytesIO
import time  # Import time module
import logging  # Import logging module

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# Configure these variables in Airflow
# Or replace with your own values for local testing
S3_BUCKET = Variable.get("s3_bucket", default_var="bdi-aircraft-yourname")
BASE_URL = Variable.get(
    "readsb_url", default_var="https://samples.adsbexchange.com/readsb-hist")
# Limit to 100 files as per requirements
MAX_FILES = int(Variable.get("max_files", default_var="100"))

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'readsb_hist_processing',
    default_args=default_args,
    description='Download and process readsb-hist data',
    schedule_interval='@monthly',  # Run once a month
    catchup=False,
    tags=["bdi", "s8", "adsb_exchange"],
)


def get_day_path(**context):
    """
    Generate the path for the target day.
    We're only processing the first day of each month.
    """
    execution_date = context['execution_date']
    year = execution_date.year
    month = execution_date.month
    day = 1  # Always the first day of the month

    # Format: YYYY/MM/DD
    day_path = f"{year}/{month:02d}/{day:02d}"
    context['ti'].xcom_push(key='day_path', value=day_path)
    context['ti'].xcom_push(
        key='date_str', value=f"{year}-{month:02d}-{day:02d}")

    return day_path


def list_files(**context):
    """
    List files available for the target day.
    """
    day_path = context['ti'].xcom_pull(key='day_path')
    url = f"{BASE_URL}/{day_path}/"

    try:
        response = requests.get(url)
        response.raise_for_status()

        # Parse HTML to find file links
        soup = BeautifulSoup(response.text, 'html.parser')
        files = []

        # Extract file names
        for link in soup.find_all('a'):
            href = link.get('href')
            if href and href.endswith('.json.gz'):
                files.append(href)

        # Sort files by name (which is by time)
        files.sort()

        # Limit to MAX_FILES
        files = files[:MAX_FILES]

        print(f"Found {len(files)} files to download")
        context['ti'].xcom_push(key='files', value=files)
        return files

    except Exception as e:
        print(f"Error listing files: {str(e)}")
        raise


def download_files(**context):
    """
    Download files from ADS-B Exchange to S3 bronze layer.
    """
    day_path = context['ti'].xcom_pull(key='day_path')
    date_str = context['ti'].xcom_pull(key='date_str')
    files = context['ti'].xcom_pull(key='files')

    # Connect to S3
    s3_hook = S3Hook(aws_conn_id='aws_default')

    base_url = f"{BASE_URL}/{day_path}/"
    bronze_prefix = f"bronze/readsb-hist/{date_str}/"

    downloaded = 0

    for file_name in files:
        try:
            # Check if file already exists in S3
            s3_key = f"{bronze_prefix}{file_name}"
            if s3_hook.check_for_key(s3_key, bucket_name=S3_BUCKET):
                print(f"File {file_name} already exists in S3, skipping...")
                downloaded += 1
                continue

            # Download file
            file_url = f"{base_url}{file_name}"
            response = requests.get(file_url)
            response.raise_for_status()

            # Upload to S3
            s3_hook.load_bytes(
                response.content,
                key=s3_key,
                bucket_name=S3_BUCKET,
                replace=True
            )

            downloaded += 1
            print(f"Downloaded {file_name} to S3 ({downloaded}/{len(files)})")

        except Exception as e:
            print(f"Error downloading {file_name}: {str(e)}")
            # Continue with next file

    print(f"Downloaded {downloaded} files to S3 bronze layer")
    return downloaded


def process_files(**context):
    """
    Process downloaded files and store in S3 silver layer.
    Extract aircraft information from the raw files.
    """
    date_str = context['ti'].xcom_pull(key='date_str')

    # Connect to S3
    s3_hook = S3Hook(aws_conn_id='aws_default')

    # List files in bronze layer
    bronze_prefix = f"bronze/readsb-hist/{date_str}/"
    silver_prefix = f"silver/readsb-hist/{date_str}/"

    # Check if processing already done
    silver_key = f"{silver_prefix}aircraft.parquet"
    if s3_hook.check_for_key(silver_key, bucket_name=S3_BUCKET):
        print(f"Silver layer data already exists, skipping processing...")
        return

    # Get list of files in bronze layer
    files = s3_hook.list_keys(
        bucket_name=S3_BUCKET,
        prefix=bronze_prefix,
        delimiter='/'
    )

    all_aircraft = []

    for file_key in files:
        if not file_key.endswith('.json.gz'):
            continue

        try:
            # Get low-level boto3 client from hook
            s3_conn = s3_hook.get_conn()
            # Call get_object directly using keyword arguments
            response = s3_conn.get_object(Bucket=S3_BUCKET, Key=file_key)
            # Read bytes directly from response
            file_bytes = response['Body'].read()

            try:
                # Attempt decompression first
                with gzip.GzipFile(fileobj=BytesIO(file_bytes), mode='rb') as f:
                    json_data = json.loads(f.read().decode('utf-8'))
                # self.log.info(f"Successfully decompressed and parsed {file_key}")
            except gzip.BadGzipFile:
                # If not gzipped (or other gzip error), try parsing directly as JSON
                # self.log.warning(f"File {file_key} not gzipped or gzip error. Attempting direct JSON parsing.")
                json_data = json.loads(file_bytes.decode('utf-8'))
                # self.log.info(f"Successfully parsed {file_key} as plain JSON.")

            # Extract time and aircraft data
            timestamp = json_data.get('now', 0)
            aircraft_list = json_data.get('aircraft', [])

            # Add timestamp to each aircraft record
            for aircraft in aircraft_list:
                aircraft['timestamp'] = timestamp

            all_aircraft.extend(aircraft_list)

        except Exception as e:
            print(f"Error processing {file_key}: {str(e)}")
            # Continue with next file

    # Convert to DataFrame for more efficient processing
    if all_aircraft:
        df = pd.DataFrame(all_aircraft)

        # Clean alt_baro: convert to numeric, coercing errors to NaN
        if 'alt_baro' in df.columns:
            df['alt_baro'] = pd.to_numeric(df['alt_baro'], errors='coerce')
        else:
            print("Warning: 'alt_baro' column not found in the DataFrame.")

        # Upload to S3 as parquet
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer)
        parquet_buffer.seek(0)

        s3_hook.load_bytes(
            parquet_buffer.getvalue(),
            key=silver_key,
            bucket_name=S3_BUCKET,
            replace=True
        )

        print(
            f"Processed {len(all_aircraft)} aircraft records from {len(files)} files")
        return len(all_aircraft)

    return 0


def prepare_data_for_api(**context):
    """
    Prepare data for the API endpoints (gold layer).
    This creates analysis-ready datasets for the /aircraft and /aircraft/{icao}/co2 endpoints.
    """
    date_str = context['ti'].xcom_pull(key='date_str')

    # Connect to S3
    s3_hook = S3Hook(aws_conn_id='aws_default')

    # Silver and gold layer paths
    silver_key = f"silver/readsb-hist/{date_str}/aircraft.parquet"
    gold_aircraft_key = f"gold/aircraft/{date_str}/data.parquet"
    gold_co2_key = f"gold/co2/{date_str}/data.parquet"

    # Check if gold data already exists
    if (s3_hook.check_for_key(gold_aircraft_key, bucket_name=S3_BUCKET) and
            s3_hook.check_for_key(gold_co2_key, bucket_name=S3_BUCKET)):
        print("Gold layer data already exists, skipping preparation...")
        return

    # Read silver data - get raw bytes, not decoded string
    # silver_data = s3_hook.read_key(silver_key, bucket_name=S3_BUCKET)
    s3_conn = s3_hook.get_conn()  # Get low-level boto3 client
    response = s3_conn.get_object(
        Bucket=S3_BUCKET, Key=silver_key)  # Call get_object directly
    file_bytes = response['Body'].read()  # Read bytes from response
    df = pd.read_parquet(BytesIO(file_bytes))

    # Prepare aircraft data
    # Extract unique aircraft with their information
    aircraft_df = df.drop_duplicates(subset=['hex']).copy()
    aircraft_columns = ['hex', 'r', 't']  # ICAO, registration, type
    aircraft_df = aircraft_df[aircraft_columns].rename(columns={
        'hex': 'icao',
        'r': 'registration',
        't': 'type'
    })

    # Prepare CO2 data
    # Group by aircraft and calculate flight duration
    # For this template, we're just counting timestamps per aircraft
    # In the real implementation, you would calculate actual flight time and CO2
    co2_df = df.groupby('hex').agg({
        'timestamp': 'count'
    }).reset_index()
    co2_df.rename(columns={
        'hex': 'icao',
        'timestamp': 'count'
    }, inplace=True)

    # For this template, we won't calculate actual CO2
    # You'll need to implement this in your solution

    # Save to gold layer
    # Aircraft data
    aircraft_buffer = BytesIO()
    aircraft_df.to_parquet(aircraft_buffer)
    aircraft_buffer.seek(0)
    s3_hook.load_bytes(
        aircraft_buffer.getvalue(),
        key=gold_aircraft_key,
        bucket_name=S3_BUCKET,
        replace=True
    )

    # CO2 data
    co2_buffer = BytesIO()
    co2_df.to_parquet(co2_buffer)
    co2_buffer.seek(0)
    s3_hook.load_bytes(
        co2_buffer.getvalue(),
        key=gold_co2_key,
        bucket_name=S3_BUCKET,
        replace=True
    )

    print(f"Prepared gold layer data: {len(aircraft_df)} aircraft records")
    return len(aircraft_df)


def load_to_database(**context):
    """
    Load processed data to PostgreSQL database for API consumption.
    """
    task_start_time = time.time()
    logger = logging.getLogger(__name__)  # Added logger
    logger.info("Starting load_to_database task.")

    date_str = context['ti'].xcom_pull(key='date_str')
    if not date_str:
        logger.error("Could not pull date_str from XCom. Aborting.")
        raise ValueError("date_str not found in XCom")

    # Connect to S3
    s3_hook = S3Hook(aws_conn_id='aws_default')

    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    # Gold layer paths
    gold_aircraft_key = f"gold/aircraft/{date_str}/data.parquet"
    gold_co2_key = f"gold/co2/{date_str}/data.parquet"

    conn = None
    cursor = None
    engine = None  # Added for to_sql
    temp_aircraft_table_name = f"temp_aircraft_{os.urandom(4).hex()}"
    temp_co2_table_name = f"temp_co2_{os.urandom(4).hex()}"

    try:
        # --- Step 1: Get SQLAlchemy Engine ---
        step_start_time = time.time()
        try:
            engine = pg_hook.get_sqlalchemy_engine()
            # Use hook attr
            logger.info(
                f"Successfully obtained SQLAlchemy engine for {pg_hook.postgres_conn_id}")
        except Exception as e:
            logger.error(
                f"Failed to get SQLAlchemy engine: {e}", exc_info=True)
            raise
        logger.info(
            f"--- Get Engine Duration: {time.time() - step_start_time:.4f} seconds ---")

        # --- Step 2: Read Gold Data from S3 ---
        step_start_time = time.time()
        logger.info(
            f"Reading gold aircraft data from s3://{S3_BUCKET}/{gold_aircraft_key}")
        s3_conn = s3_hook.get_conn()
        aircraft_obj = s3_conn.get_object(
            Bucket=S3_BUCKET, Key=gold_aircraft_key)
        aircraft_data_bytes = aircraft_obj['Body'].read()
        aircraft_df = pd.read_parquet(BytesIO(aircraft_data_bytes))
        logger.info(f"Read {len(aircraft_df)} aircraft records.")

        logger.info(
            f"Reading gold CO2 data from s3://{S3_BUCKET}/{gold_co2_key}")
        co2_obj = s3_conn.get_object(Bucket=S3_BUCKET, Key=gold_co2_key)
        co2_data_bytes = co2_obj['Body'].read()
        co2_df = pd.read_parquet(BytesIO(co2_data_bytes))
        logger.info(f"Read {len(co2_df)} CO2 records.")
        logger.info(
            f"--- S3 Read Duration: {time.time() - step_start_time:.4f} seconds ---")

        # --- Step 3: Create Target Tables (if not exist) ---
        step_start_time = time.time()
        logger.info("Ensuring target tables aircraft and aircraft_co2 exist...")
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS aircraft (
                icao VARCHAR(10) PRIMARY KEY,
                registration VARCHAR(20),
                type VARCHAR(10)
            );
            
            CREATE TABLE IF NOT EXISTS aircraft_co2 (
                icao VARCHAR(10),
                day DATE,
                count INTEGER,
                PRIMARY KEY (icao, day)
            );
        """
        pg_hook.run(create_table_sql)
        logger.info("Tables ensured.")
        logger.info(
            f"--- Create Tables Duration: {time.time() - step_start_time:.4f} seconds ---")

        # --- Step 4: Load DataFrames to Temporary Tables ---
        step_start_time = time.time()
        if not aircraft_df.empty:
            logger.info(
                f"Loading {len(aircraft_df)} aircraft records into temporary table {temp_aircraft_table_name}...")
            aircraft_df.to_sql(name=temp_aircraft_table_name, con=engine,
                               if_exists='replace', index=False, method='multi', chunksize=10000)
            logger.info(
                f"Successfully loaded aircraft data into {temp_aircraft_table_name}.")
        else:
            logger.warning(
                "Aircraft DataFrame is empty, skipping temp table load.")

        if not co2_df.empty:
            logger.info(
                f"Preparing {len(co2_df)} CO2 records for temp table...")
            co2_df['day'] = date_str  # Add day column
            logger.info(
                f"Loading {len(co2_df)} CO2 records into temporary table {temp_co2_table_name}...")
            co2_df.to_sql(name=temp_co2_table_name, con=engine,
                          if_exists='replace', index=False, method='multi', chunksize=10000)
            logger.info(
                f"Successfully loaded CO2 data into {temp_co2_table_name}.")
        else:
            logger.warning("CO2 DataFrame is empty, skipping temp table load.")
        logger.info(
            f"--- Temp Table Load Duration: {time.time() - step_start_time:.4f} seconds ---")

        # --- Step 5: Execute UPSERT from Temporary Tables ---
        step_start_time = time.time()
        # UPSERT for Aircraft table
        if not aircraft_df.empty:
            logger.info(
                f"Executing UPSERT from {temp_aircraft_table_name} to aircraft...")
            aircraft_cols = [f'\"{col}\"' for col in aircraft_df.columns]
            aircraft_update_set = ", ".join(
                [f'\"{col}\" = EXCLUDED.\"{col}\"' for col in aircraft_df.columns if col != 'icao'])
            upsert_aircraft_sql = f"""
            INSERT INTO aircraft ({', '.join(aircraft_cols)})
            SELECT {', '.join(aircraft_cols)} FROM \"{temp_aircraft_table_name}\"
            ON CONFLICT (\"icao\") DO UPDATE SET
              {aircraft_update_set};
            """
            logger.debug(f"UPSERT Aircraft SQL: {upsert_aircraft_sql}")
            # Autocommit for UPSERT
            pg_hook.run(upsert_aircraft_sql, autocommit=True)
            logger.info(f"UPSERT to aircraft completed.")
        else:
            logger.info(
                "Skipping UPSERT to aircraft table as temp table was not created.")

        # UPSERT for CO2 table
        if not co2_df.empty:
            logger.info(
                f"Executing UPSERT from {temp_co2_table_name} to aircraft_co2...")
            # Use columns from df after adding 'day'
            co2_cols = [f'\"{col}\"' for col in co2_df.columns]
            co2_update_set = ", ".join(
                [f'\"{col}\" = EXCLUDED.\"{col}\"' for col in co2_df.columns if col not in ['icao', 'day']])
            upsert_co2_sql = f"""
            INSERT INTO aircraft_co2 ({', '.join(co2_cols)})
            SELECT {', '.join([f'\"{col}\"' if col != 'day' else '\"day\"::date' for col in co2_df.columns])} FROM \"{temp_co2_table_name}\"
            ON CONFLICT (\"icao\", \"day\") DO UPDATE SET
              {co2_update_set};
            """
            logger.debug(f"UPSERT CO2 SQL: {upsert_co2_sql}")
            # Autocommit for UPSERT
            pg_hook.run(upsert_co2_sql, autocommit=True)
            logger.info(f"UPSERT to aircraft_co2 completed.")
        else:
            logger.info(
                "Skipping UPSERT to aircraft_co2 table as temp table was not created.")
        logger.info(
            f"--- UPSERT Duration: {time.time() - step_start_time:.4f} seconds ---")

        # Removed explicit transaction commit/rollback here as UPSERTS are autocommitted

    except Exception as e:
        logger.error(
            f"Error in database load task pipeline: {str(e)}", exc_info=True)
        raise
    finally:
        # --- Step 6: Drop Temporary Tables and Close Connection ---
        step_start_time = time.time()
        if engine:  # Ensure engine was successfully created before trying to drop
            try:
                with pg_hook.get_conn() as conn:
                    with conn.cursor() as cur:
                        if not aircraft_df.empty:
                            logger.info(
                                f"Dropping temporary table {temp_aircraft_table_name}...")
                            cur.execute(
                                f'DROP TABLE IF EXISTS \"{temp_aircraft_table_name}\";')
                            logger.info(
                                f"Successfully dropped {temp_aircraft_table_name}.")
                        if not co2_df.empty:
                            logger.info(
                                f"Dropping temporary table {temp_co2_table_name}...")
                            cur.execute(
                                f'DROP TABLE IF EXISTS \"{temp_co2_table_name}\";')
                            logger.info(
                                f"Successfully dropped {temp_co2_table_name}.")
            except Exception as drop_err:
                logger.warning(
                    f"Could not drop one or more temporary tables: {drop_err}")
        else:
            logger.warning(
                "Skipping drop of temp tables as engine was not initialized.")
        # Closing the engine connection pool is usually managed by Airflow/SQLAlchemy
        logger.info(
            f"--- Cleanup/Connection Close Duration: {time.time() - step_start_time:.4f} seconds ---")

    logger.info(
        f"Finished load_to_database task. Total Duration: {time.time() - task_start_time:.4f} seconds")


# Define the tasks
get_day_path_task = PythonOperator(
    task_id='get_day_path',
    python_callable=get_day_path,
    provide_context=True,
    dag=dag,
)

list_files_task = PythonOperator(
    task_id='list_files',
    python_callable=list_files,
    provide_context=True,
    dag=dag,
)

download_task = PythonOperator(
    task_id='download_files',
    python_callable=download_files,
    provide_context=True,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_files',
    python_callable=process_files,
    provide_context=True,
    dag=dag,
)

prepare_api_data_task = PythonOperator(
    task_id='prepare_api_data',
    python_callable=prepare_data_for_api,
    provide_context=True,
    dag=dag,
)

load_db_task = PythonOperator(
    task_id='load_to_database',
    python_callable=load_to_database,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
get_day_path_task >> list_files_task >> download_task >> process_task >> prepare_api_data_task >> load_db_task
