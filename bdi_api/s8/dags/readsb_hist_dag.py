import pendulum
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import os
import gzip
import io
import json
import pandas as pd
from typing import List, Optional, Tuple
import numpy as np
import time  # Import the time module

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException

# --- Constants & Configuration ---
# Use the 'samples' URL and don't include trailing slash
ADS_B_EXCHANGE_HISTORICAL_URL = "https://samples.adsbexchange.com/readsb-hist"
S3_BUCKET_NAME = "bdi-s8-data-lake-5103"  # From Terraform output
S3_CONN_ID = "aws_default"  # Assumes an S3 connection configured in Airflow
# Assumes a Postgres connection configured in Airflow
POSTGRES_CONN_ID = "postgres_default"
RDS_TARGET_TABLE = "positions"
BRONZE_PREFIX = "bronze/readsb-hist"
SILVER_PREFIX = "silver/readsb-hist"
# GOLD_PREFIX = "gold/readsb-hist" # Define if/when gold processing is implemented
DAILY_FILE_LIMIT = 100  # Max files to process per DAG run

logger = logging.getLogger(__name__)

# --- Task Functions ---


@task
def get_date_specific_url(base_url: str, logical_date: pendulum.DateTime) -> Tuple[str, str]:
    """(Task) Constructs the URL and date path string for the 1st day of the execution month."""
    year = logical_date.year
    month = logical_date.month
    day = 1
    date_path = f"{year:04d}/{month:02d}/{day:02d}"
    full_url = f"{base_url}/{date_path}/"
    logger.info(f"Constructed date-specific URL: {full_url}")
    logger.info(f"Date path string: {date_path}")
    return full_url, date_path  # Return both URL and date path string


@task
def get_file_list_from_html(url_info: Tuple[str, str]) -> List[str]:
    """(Task) Fetches .json.gz file URLs from the specific date directory listing."""
    data_url, _ = url_info  # Unpack the tuple, we only need the URL here
    logger.info(f"Fetching directory listing from {data_url}")
    file_urls = []
    try:
        response = requests.get(data_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a')
        logger.info(f"Found {len(links)} links on the page.")
        for link in links:
            href = link.get('href')
            if href and href.endswith('.json.gz'):
                # Use requests.compat.urljoin to handle relative links correctly
                full_url = requests.compat.urljoin(data_url, href)
                file_urls.append(full_url)
        logger.info(f"Found {len(file_urls)} .json.gz files.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching URL {data_url}: {e}")
        raise  # Re-raise to fail the task
    except Exception as e:
        logger.error(f"Error parsing HTML content from {data_url}: {e}")
        raise  # Re-raise to fail the task
    if not file_urls:
        logger.warning("No .json.gz files found in the directory listing.")
        # Depending on desired behavior, could raise an exception or allow DAG to finish
    return file_urls


@task
def check_existing_files_in_bronze(url_info: Tuple[str, str], file_urls: List[str], bucket_name: str, s3_conn_id: str) -> List[str]:
    """(Task) Checks S3 bronze layer for existing files using a single list operation and returns URLs for files not yet downloaded."""
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    files_to_download = []

    # Extract date_path from url_info to build the correct prefix
    _, date_path = url_info
    s3_prefix = f"{BRONZE_PREFIX}/{date_path}"
    logger.info(
        f"Listing existing files in s3://{bucket_name}/{s3_prefix}")

    try:
        # List all keys in the target S3 prefix ONCE
        existing_keys = s3_hook.list_keys(
            bucket_name=bucket_name, prefix=s3_prefix)
        # Extract just the filenames for efficient lookup
        existing_filenames = set(os.path.basename(key)
                                 for key in existing_keys or [])
        logger.info(
            f"Found {len(existing_filenames)} existing files in S3 prefix.")
    except Exception as e:
        logger.error(f"Error listing keys in S3 prefix {s3_prefix}: {e}")
        existing_filenames = set()  # Assume no files exist if listing fails

    # Compare website URLs against existing S3 filenames
    for file_url in file_urls:
        filename = os.path.basename(urlparse(file_url).path)
        if filename not in existing_filenames:
            files_to_download.append(file_url)

    logger.info(
        f"{len(files_to_download)} files identified for download (out of {len(file_urls)}). ")

    # Apply the limit *before* returning
    limited_files = files_to_download[:DAILY_FILE_LIMIT]
    logger.info(
        f"Applying limit: {len(limited_files)} files will be processed.")

    if not limited_files:
        raise AirflowSkipException(
            "No new files to download or limit reached.")
    return limited_files


@task
def download_and_upload_to_s3(file_url: str, bucket_name: str, s3_conn_id: str):
    """(Task) Downloads a single file and uploads it to S3 bronze layer, inferring the date path."""
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    parsed_url = urlparse(file_url)
    path_parts = parsed_url.path.strip('/').split('/')

    # Infer date path and filename (e.g., readsb-hist/YYYY/MM/DD/filename.json.gz)
    if len(path_parts) >= 5:
        # Assumes URL structure like .../readsb-hist/YYYY/MM/DD/filename.json.gz
        date_path = "/".join(path_parts[-4:-1])  # YYYY/MM/DD
        filename = path_parts[-1]
        s3_prefix = f"{BRONZE_PREFIX}/{date_path}"
        s3_key = f"{s3_prefix}/{filename}"
        logger.info(f"Target S3 Key: s3://{bucket_name}/{s3_key}")
    else:
        # Fallback or error if URL structure is unexpected
        logger.error(
            f"Could not infer date path from URL structure: {file_url}. Using base prefix.")
        filename = os.path.basename(parsed_url.path)
        # WARNING: This fallback writes to the base prefix, potentially incorrect
        s3_key = f"{BRONZE_PREFIX}/{filename}"

    logger.info(f"Attempting download: {file_url}")
    try:
        # Download step
        logger.debug(f"Initiating download request for {file_url}")
        response = requests.get(file_url, stream=True, timeout=60)
        logger.debug(
            f"Download request completed with status: {response.status_code}")
        response.raise_for_status()

        # Upload step
        logger.debug(f"Initiating S3 upload to {s3_key}")
        s3_hook.load_file_obj(file_obj=response.raw,
                              key=s3_key, bucket_name=bucket_name, replace=True)
        logger.info(
            f"Successfully downloaded and uploaded {filename} to s3://{bucket_name}/{s3_key}")
        return s3_key  # Return the key of the downloaded file

    except requests.exceptions.Timeout as e:
        logger.error(f"Timeout error downloading {file_url}: {e}")
        raise
    except requests.exceptions.HTTPError as e:
        logger.error(
            f"HTTP error downloading {file_url}: Status {e.response.status_code} - {e.response.reason}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"General request error downloading {file_url}: {e}")
        raise
    except Exception as e:
        # Catch potential S3Hook or other errors during upload
        logger.error(
            f"Error during processing/upload of {filename} (from {file_url}) to S3 key {s3_key}: {e}", exc_info=True)
        raise


@task
def process_bronze_to_silver(bronze_s3_key: str, bucket_name: str, s3_conn_id: str) -> Optional[str]:
    """(Task) Reads a bronze .json.gz file, processes it, and writes a Parquet file to the silver layer, inferring date path."""
    if bronze_s3_key is None:
        logger.warning(
            "No bronze S3 key provided, skipping silver processing.")
        return None

    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    logger.info(f"Processing bronze file: s3://{bucket_name}/{bronze_s3_key}")
    try:
        # Get file content from S3
        file_obj = s3_hook.get_key(key=bronze_s3_key, bucket_name=bucket_name)
        file_content = file_obj.get()['Body'].read()

        # Decompress (if gzipped) and parse the *entire* JSON object
        data = []
        try:
            # First, try to decompress assuming it's gzipped
            with gzip.GzipFile(fileobj=io.BytesIO(file_content), mode='rb') as gz_file:
                decompressed_content = gz_file.read()
            logger.debug(f"Successfully decompressed {bronze_s3_key}.")
            # Now parse the decompressed content as a single JSON object
            full_json = json.loads(decompressed_content.decode('utf-8'))
            logger.debug(
                f"Successfully parsed JSON from decompressed content.")

        except gzip.BadGzipFile:
            logger.warning(
                f"File {bronze_s3_key} not gzipped or gzip error. Attempting direct JSON parsing of raw content.")
            # If not gzipped, try parsing the original content directly
            try:
                full_json = json.loads(file_content.decode('utf-8'))
                logger.debug(
                    f"Successfully parsed JSON directly from raw content.")
            except json.JSONDecodeError as direct_err:
                logger.error(
                    f"Could not parse {bronze_s3_key} as Gzip or direct JSON: {direct_err}")
                raise  # Re-raise if parsing fails in both attempts
        except json.JSONDecodeError as json_err:
            logger.error(
                f"Failed to decode JSON from {bronze_s3_key} even after decompression attempt: {json_err}")
            raise  # Re-raise if JSON is invalid

        # Extract the 'aircraft' list from the parsed JSON object
        if isinstance(full_json, dict) and 'aircraft' in full_json:
            data = full_json.get('aircraft', [])
            # Add 'now' timestamp to each record if available (consistent with previous logic)
            now_timestamp = full_json.get('now')
            if now_timestamp and data:
                logger.debug(
                    f"Adding 'now' timestamp ({now_timestamp}) to records.")
                for record in data:
                    # Avoid overwriting if 'timestamp' already exists from rename
                    if 'timestamp' not in record:
                        record['timestamp'] = now_timestamp
            elif not data:
                logger.warning(f"'aircraft' list is empty in {bronze_s3_key}.")
            else:  # 'now' timestamp missing
                logger.debug(
                    f"'now' timestamp not found at top level of {bronze_s3_key}.")
        elif isinstance(full_json, list):
            # Handle case where the file might *just* be a list of aircraft (unlikely based on sample, but safe)
            logger.warning(
                f"Parsed JSON in {bronze_s3_key} is a list, not a dict with 'aircraft' key. Using list directly.")
            data = full_json
        else:
            logger.error(
                f"Parsed JSON from {bronze_s3_key} is not a dict with 'aircraft' key or a list.")
            raise ValueError("Unexpected JSON structure")

        if not data:
            logger.warning(
                f"No valid data extracted from {bronze_s3_key}. Skipping silver write.")
            return None

        df = pd.DataFrame(data)
        logger.info(
            f"Created DataFrame with shape {df.shape} from {bronze_s3_key}")

        # --- Rename, Generate ID, and Clean/Transform Data --- #

        # Handle potential duplicate 'timestamp' before renaming 'seen'
        if 'timestamp' in df.columns and 'seen' in df.columns:
            logger.warning(
                "Both 'timestamp' and 'seen' columns exist in source data. Dropping 'seen' column.")
            df.drop(columns=['seen'], inplace=True)
            # Only rename 'hex' if 'timestamp' already exists
            rename_map = {'hex': 'icao'}
        elif 'seen' in df.columns:
            # Only rename 'seen' if 'timestamp' doesn't exist
            rename_map = {'hex': 'icao', 'seen': 'timestamp'}
        else:
            # Neither 'seen' nor 'timestamp' exists initially (unlikely, but safe)
            rename_map = {'hex': 'icao'}

        # Rename columns to match target schema
        # Check which columns actually exist before trying to rename
        actual_rename_map = {k: v for k,
                             v in rename_map.items() if k in df.columns}
        if actual_rename_map:
            df.rename(columns=actual_rename_map, inplace=True)
            logger.info(f"Renamed columns: {actual_rename_map}")

        # Generate 'id' column if icao and timestamp exist (using vectorized operations)
        if 'icao' in df.columns and 'timestamp' in df.columns:
            logger.debug(
                "Generating 'id' column from 'icao' and 'timestamp' using vectorized operations...")
            # Use vectorized operations for efficiency
            df['icao_str'] = df['icao'].astype(str)
            df['timestamp_str'] = df['timestamp'].astype(str)
            df['id'] = df['icao_str'] + '_' + df['timestamp_str']
            # Set id to None where original icao or timestamp was null
            df.loc[df['icao'].isna() | df['timestamp'].isna(), 'id'] = None
            df.drop(columns=['icao_str', 'timestamp_str'],
                    inplace=True)  # Drop temporary columns
            logger.debug("Finished generating 'id' column.")
        else:
            logger.warning(
                "Could not generate 'id' column: 'icao' or 'timestamp' not present after rename.")

        # --- Data Cleaning --- #
        # Clean alt_baro: Convert non-numeric values (like 'ground') to NaN
        if 'alt_baro' in df.columns:
            original_type = df['alt_baro'].dtype
            df['alt_baro'] = pd.to_numeric(df['alt_baro'], errors='coerce')
            # Log if type changed or NaNs were introduced
            if df['alt_baro'].dtype != original_type or df['alt_baro'].isna().any():
                logger.info(
                    "Cleaned 'alt_baro' column: converted non-numeric values to NaN.")
        # Add other data cleaning/transformation steps here as needed
        # ----------------------

        # Define silver layer S3 key by inferring date path
        path_parts = bronze_s3_key.split('/')
        silver_s3_key = None
        if len(path_parts) >= 4:
            # Assumes bronze_s3_key like bronze/readsb-hist/YYYY/MM/DD/filename.json.gz
            date_path = "/".join(path_parts[-4:-1])  # YYYY/MM/DD
            base_filename = path_parts[-1].replace('.json.gz', '.parquet')
            silver_s3_key = f"{SILVER_PREFIX}/{date_path}/{base_filename}"
            logger.info(f"Inferred Silver S3 Key: {silver_s3_key}")
        else:
            logger.error(
                f"Could not infer date path from bronze key: {bronze_s3_key}")
            return None  # Cannot determine where to write silver file

        # Write DataFrame to Parquet in memory
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        # Upload Parquet file to S3 silver layer
        logger.info(
            f"Writing silver file to s3://{bucket_name}/{silver_s3_key}")
        s3_hook.load_file_obj(
            file_obj=parquet_buffer, key=silver_s3_key, bucket_name=bucket_name, replace=True)

        logger.info(
            f"Successfully processed {bronze_s3_key} to {silver_s3_key}")
        return silver_s3_key

    except Exception as e:
        logger.error(f"Error processing file {bronze_s3_key} to silver: {e}")
        raise  # Fail the task


@task
def load_silver_to_rds(silver_s3_key: str, bucket_name: str, table_name: str, s3_conn_id: str, postgres_conn_id: str):
    """(Task) Reads a silver Parquet file and loads its data into RDS with detailed timing."""
    task_start_time = time.time()
    if silver_s3_key is None:  # Handle potential skipped upstream tasks
        logger.warning("No silver S3 key provided, skipping RDS load.")
        return

    logger.info(f"Starting RDS load for: s3://{bucket_name}/{silver_s3_key}")
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    engine = None
    temp_table_name = f"temp_{table_name}_{os.urandom(4).hex()}"
    logger.info(f"Using temporary table name: {temp_table_name}")

    try:
        # --- Step 1: Get SQLAlchemy Engine ---
        step_start_time = time.time()
        try:
            engine = pg_hook.get_sqlalchemy_engine()
            logger.info(
                f"Successfully obtained SQLAlchemy engine for {postgres_conn_id}")
        except Exception as e:
            logger.error(f"Failed to get SQLAlchemy engine: {e}")
            raise
        logger.info(
            f"--- Get Engine Duration: {time.time() - step_start_time:.4f} seconds ---")

        # --- Step 2: Read Silver Parquet from S3 ---
        step_start_time = time.time()
        logger.info(
            f"Reading silver data from S3: s3://{bucket_name}/{silver_s3_key}")
        file_obj = s3_hook.get_key(key=silver_s3_key, bucket_name=bucket_name)
        silver_df = pd.read_parquet(io.BytesIO(file_obj.get()['Body'].read()))
        logger.info(f"Read {len(silver_df)} rows from {silver_s3_key}.")
        logger.info(
            f"--- S3 Read Duration: {time.time() - step_start_time:.4f} seconds ---")

        if silver_df.empty:
            logger.warning(
                f"DataFrame from {silver_s3_key} is empty, skipping load to table {table_name}.")
            return

        # --- Step 3: Prepare DataFrame --- (Timing included in S3 read for simplicity, usually fast)
        target_columns = ['id', 'icao', 'timestamp', 'lat', 'lon']
        available_target_columns = [
            col for col in target_columns if col in silver_df.columns]
        if not available_target_columns or 'id' not in available_target_columns:
            logger.warning(
                f"Not all target RDS columns ({target_columns}) found in {silver_s3_key}, or missing 'id'. Skipping load.")
            return

        rds_df = silver_df[available_target_columns].copy()
        rds_df = rds_df.replace({pd.NaT: None})
        rds_df = rds_df.where(pd.notnull(rds_df), None)
        logger.info(
            f"Prepared DataFrame with {len(rds_df)} rows for temp table.")

        # --- Step 4: Load to Temporary Table ---
        step_start_time = time.time()
        logger.info(
            f"Loading {len(rds_df)} rows into temporary table {temp_table_name}...")
        rds_df.to_sql(name=temp_table_name, con=engine, if_exists='replace',
                      index=False, method='multi', chunksize=10000)
        logger.info(
            f"Successfully loaded data into temporary table {temp_table_name}.")
        logger.info(
            f"--- Temp Table Load Duration: {time.time() - step_start_time:.4f} seconds ---")

        # --- Step 5: Execute UPSERT from Temp Table ---
        step_start_time = time.time()
        # Ensure column names are correctly quoted for SQL
        quoted_target_columns = [
            f'\"{col}\"' for col in available_target_columns]
        quoted_id_column = '\"id\"'  # Specific handling for the conflict target
        update_set_statements = ", ".join(
            [f'\"{col}\" = EXCLUDED.\"{col}\"' for col in available_target_columns if col != 'id']
        )

        upsert_sql = f"""
        INSERT INTO \"{table_name}\" ({', '.join(quoted_target_columns)})
        SELECT {', '.join(quoted_target_columns)} FROM \"{temp_table_name}\"
        ON CONFLICT ({quoted_id_column}) DO UPDATE SET
          {update_set_statements};
        """
        logger.info(
            f"Executing UPSERT from {temp_table_name} to {table_name}...")
        logger.debug(f"UPSERT SQL: {upsert_sql}")
        pg_hook.run(upsert_sql, autocommit=True)
        logger.info(
            f"Successfully upserted data from {temp_table_name} to {table_name}.")
        logger.info(
            f"--- UPSERT Duration: {time.time() - step_start_time:.4f} seconds ---")

    except Exception as e:
        logger.error(
            f"Error during RDS load pipeline for {silver_s3_key}: {e}", exc_info=True)
        raise
    finally:
        # --- Step 6: Drop Temporary Table ---
        step_start_time = time.time()
        if engine:  # Ensure engine was successfully created before trying to drop
            try:
                logger.info(f"Dropping temporary table {temp_table_name}...")
                with pg_hook.get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            f'DROP TABLE IF EXISTS \"{temp_table_name}\";')
                logger.info(
                    f"Successfully dropped temporary table {temp_table_name}.")
            except Exception as drop_err:
                logger.warning(
                    f"Could not drop temporary table {temp_table_name}: {drop_err}")
            logger.info(
                f"--- Drop Temp Table Duration: {time.time() - step_start_time:.4f} seconds ---")
        else:
            logger.warning(
                f"Skipping drop of temp table {temp_table_name} as engine was not initialized.")

    logger.info(
        f"Finished RDS load for {silver_s3_key}. Total task duration: {time.time() - task_start_time:.4f} seconds")

# --- DAG Definition ---


@dag(
    dag_id="readsb_historical_data_pipeline",
    start_date=pendulum.datetime(2024, 7, 30, tz="UTC"),  # Adjust start date
    schedule="@monthly",  # Changed from @daily as per README
    catchup=False,
    tags=["bdi", "s8", "adsb_exchange"],
    # Allow up to 16 mapped task instances to run concurrently for this DAG
    max_active_tasks=8,
    doc_md="""
    ### ADS-B Exchange Historical Data Pipeline
    
    This DAG downloads historical flight data files from ADS-B Exchange, processes them,
    and loads the results into S3 (bronze/silver) and an RDS database.
    
    **Steps:**
    1. Fetches the list of available `.json.gz` files from the ADS-B Exchange website.
    2. Checks which files already exist in the S3 bronze layer (basic idempotency).
    3. Downloads new files (up to a daily limit) to the S3 bronze layer.
    4. Processes each downloaded bronze file (JSON Lines) into a Parquet file in the S3 silver layer.
    5. Loads the data from each silver Parquet file into a target table in RDS.
    
    **Connections Required:**
    - `aws_default`: AWS Connection for S3 access.
    - `postgres_default`: Postgres Connection for RDS access.
    """,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=2),
        # "aws_conn_id": S3_CONN_ID,  # Removed - Pass via .partial()
        # "postgres_conn_id": POSTGRES_CONN_ID  # Removed - Pass via .partial()
    }
)
def readsb_historical_data_pipeline():

    # 0. Get the date-specific URL and date path string
    # The logical_date is automatically injected
    date_info_output = get_date_specific_url(
        base_url=ADS_B_EXCHANGE_HISTORICAL_URL)

    # 1. Get all available files from the date-specific website URL
    # Pass the output object from the previous task directly
    all_file_urls_output = get_file_list_from_html(url_info=date_info_output)

    # 2. Filter out already downloaded files (check bronze layer)
    # Pass the outputs from the previous tasks directly
    limited_files_output = check_existing_files_in_bronze(
        url_info=date_info_output,
        file_urls=all_file_urls_output,
        bucket_name=S3_BUCKET_NAME,
        s3_conn_id=S3_CONN_ID
    )

    # 3. Download new files, process to silver, load to RDS (dynamic tasks)
    # Use the output object from check_existing_files_in_bronze for expansion
    downloaded_bronze_keys = download_and_upload_to_s3.partial(
        bucket_name=S3_BUCKET_NAME,
        s3_conn_id=S3_CONN_ID
    ).expand(file_url=limited_files_output)

    # Similar modification needed for process_bronze_to_silver prefix handling
    processed_silver_keys = process_bronze_to_silver.partial(
        bucket_name=S3_BUCKET_NAME,
        s3_conn_id=S3_CONN_ID
    ).expand(bronze_s3_key=downloaded_bronze_keys)

    # load_silver_to_rds doesn't directly use the date prefix, but the input key includes it
    load_silver_to_rds.partial(
        bucket_name=S3_BUCKET_NAME,
        table_name=RDS_TARGET_TABLE,
        s3_conn_id=S3_CONN_ID,
        postgres_conn_id=POSTGRES_CONN_ID
    ).expand(silver_s3_key=processed_silver_keys)


# Instantiate the DAG
readsb_historical_data_pipeline()
