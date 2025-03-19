from fastapi import APIRouter, status, Depends
import boto3
import json
import pandas as pd
import time
import logging
import asyncio
from sqlalchemy.orm import Session

from bdi_api.settings import DBCredentials, Settings
from .models import get_db, create_tables, Aircraft, Position, Statistics

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

settings = Settings()

# Try to get credentials from environment variables
try:
    # Try to use environment variables first
    db_credentials = DBCredentials()
except Exception as e:
    # If it fails, use hardcoded values for local development
    print(f"Error loading DBCredentials in exercise.py: {e}")
    print("Using default values for local connection...")

    # Temporary class to simulate DBCredentials
    class LocalDBCredentials:
        def __init__(self):
            self.host = "localhost"
            self.port = 5432
            self.username = "bdiadmin"
            self.password = "postgres123"

    db_credentials = LocalDBCredentials()

BASE_URL = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"

s7 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s7",
    tags=["s7"],
)


@s7.post("/aircraft/prepare")
async def prepare_data(db: Session = Depends(get_db)) -> str:
    """Get the raw data from s3 and insert it into RDS

    Use credentials passed from `db_credentials`
    """
    time_start = time.time()
    logger.info("Starting data preparation from S3 to PostgreSQL")

    # Ensure tables exist
    create_tables()

    # S3 Configuration
    s3_client = boto3.client('s3')
    s3_bucket = settings.s3_bucket
    raw_prefix = "raw/day=20231101/"

    # Clear existing data in the database
    try:
        db.query(Aircraft).delete()
        db.query(Position).delete()
        db.query(Statistics).delete()
        db.commit()
        logger.info("Cleaned existing data from database")
    except Exception as e:
        db.rollback()
        logger.error(f"Error cleaning database: {str(e)}")
        raise

    async def process_file(s3_key: str) -> pd.DataFrame:
        try:
            # Download file from S3
            response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
            json_content = response['Body'].read()
            data = json.loads(json_content)

            df_raw = pd.DataFrame([{
                'raw_data': json.dumps(data),
                'timestamp': data['now'],
            }])

            # Process all rows at once using apply
            def process_row(row):
                data = json.loads(row['raw_data'])
                return pd.Series({
                    'timestamp': data['now'],
                    'aircraft_data': data['aircraft']
                })

            # Vectorized processing
            processed = df_raw.apply(process_row, axis=1)

            # Explode aircraft_data to create a DataFrame with all aircraft
            df = pd.DataFrame(processed['aircraft_data'].explode().tolist())
            # broadcast timestamp
            df['timestamp'] = processed['timestamp'].iloc[0]

            return df
        except Exception as e:
            logger.error(f"Error processing file {s3_key}: {str(e)}")
            raise

    # List all raw files
    paginator = s3_client.get_paginator('list_objects_v2')
    raw_files = []
    for page in paginator.paginate(Bucket=s3_bucket, Prefix=raw_prefix):
        if 'Contents' in page:
            raw_files.extend([obj['Key'] for obj in page['Contents']])

    # Process files in chunks to optimize memory
    chunk_size = 50
    all_aircraft_info = []
    all_positions = []
    all_stats = []

    for i in range(0, len(raw_files), chunk_size):
        chunk_files = raw_files[i:i + chunk_size]
        tasks = [process_file(file) for file in chunk_files]
        chunk_dfs = await asyncio.gather(*tasks)

        # Combine chunk results
        df = pd.concat(chunk_dfs, ignore_index=True)

        # Extract aircraft information (vectorized)
        aircraft_info = df[['hex', 'r', 't']].drop_duplicates()
        aircraft_info = aircraft_info.sort_values('hex')
        aircraft_info.columns = ['icao', 'registration', 'type']
        all_aircraft_info.append(aircraft_info)

        # Extract positions (vectorized)
        positions = df[['hex', 'timestamp', 'lat', 'lon']
                       ].copy().sort_values(['hex', 'timestamp'])
        positions.columns = ['icao', 'timestamp', 'lat', 'lon']
        # Add a unique ID for each position based on icao+timestamp
        positions['id'] = positions.apply(
            lambda x: f"{x['icao']}_{x['timestamp']}", axis=1)
        all_positions.append(positions)

        # Calculate statistics (vectorized)
        df['emergency'] = df['emergency'].fillna('none')
        df['had_emergency'] = (df['emergency'] != 'none')
        df['alt_baro'] = pd.to_numeric(df['alt_baro'], errors='coerce')
        df['gs'] = pd.to_numeric(df['gs'], errors='coerce')

        stats = df.groupby('hex').agg({
            'alt_baro': 'max',
            'gs': 'max',
            'had_emergency': 'max'
        }).reset_index()
        stats.columns = ['icao', 'max_altitude_baro',
                         'max_ground_speed', 'had_emergency']
        all_stats.append(stats)

        logger.info(
            f"Processed chunk {i//chunk_size + 1} of {(len(raw_files) + chunk_size - 1)//chunk_size}")

    # Bulk insert into database
    try:
        # Combine all results
        final_aircraft = pd.concat(
            all_aircraft_info, ignore_index=True).drop_duplicates().sort_values('icao')
        final_positions = pd.concat(
            all_positions, ignore_index=True).sort_values(['icao', 'timestamp'])
        final_stats = pd.concat(all_stats, ignore_index=True).groupby('icao').agg({
            'max_altitude_baro': 'max',
            'max_ground_speed': 'max',
            'had_emergency': 'max'
        }).reset_index()

        # Insert into database
        # Aircraft
        aircraft_records = final_aircraft.to_dict('records')
        db.bulk_insert_mappings(Aircraft, aircraft_records)

        # Positions
        positions_records = final_positions.to_dict('records')
        db.bulk_insert_mappings(Position, positions_records)

        # Statistics
        stats_records = final_stats.to_dict('records')
        db.bulk_insert_mappings(Statistics, stats_records)

        db.commit()
        logger.info("Successfully inserted all data into PostgreSQL")
    except Exception as e:
        db.rollback()
        logger.error(f"Error inserting data into database: {str(e)}")
        raise

    time_end = time.time()
    logger.info(f"Execution time: {time_end - time_start}")

    return "OK"


@s7.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0, db: Session = Depends(get_db)) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc FROM THE DATABASE

    Use credentials passed from `db_credentials`
    """
    skip = page * num_results
    aircraft = db.query(Aircraft).order_by(
        Aircraft.icao).offset(skip).limit(num_results).all()
    return [{"icao": a.icao, "registration": a.registration, "type": a.type} for a in aircraft]


@s7.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0, db: Session = Depends(get_db)) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list. FROM THE DATABASE

    Use credentials passed from `db_credentials`
    """
    skip = page * num_results
    positions = db.query(Position).filter(Position.icao == icao).order_by(
        Position.timestamp).offset(skip).limit(num_results).all()
    return [{"timestamp": p.timestamp, "lat": p.lat, "lon": p.lon} for p in positions]


@s7.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str, db: Session = Depends(get_db)) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed
    * had_emergency

    FROM THE DATABASE

    Use credentials passed from `db_credentials`
    """
    # Optimized query for performance
    stats = db.query(Statistics).filter(Statistics.icao == icao).first()
    if not stats:
        return {"max_altitude_baro": 0, "max_ground_speed": 0, "had_emergency": False}

    return {
        "max_altitude_baro": stats.max_altitude_baro,
        "max_ground_speed": stats.max_ground_speed,
        "had_emergency": stats.had_emergency
    }
