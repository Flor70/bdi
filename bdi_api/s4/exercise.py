import asyncio
import json
import logging
import time
from io import BytesIO
from typing import Annotated

import boto3
import httpx
import pandas as pd
from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings

settings = Settings()

# Configuração do logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.handlers = [console_handler]

s4 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s4",
    tags=["s4"],
)

@s4.post("/aircraft/download")
async def download_data(
    file_limit: Annotated[
        int,
        Query(
            ...,
            description="""
    Limits the number of files to download.
    You must always start from the first the page returns and
    go in ascending order in order to correctly obtain the results.
    I'll test with increasing number of files starting from 100.""",
        ),
    ] = 100,
) -> str:
    """Same as s1 but store to an aws s3 bucket taken from settings
    and inside the path `raw/day=20231101/`
    """
    time_start = time.time()
    logger.info(f"Starting download of {file_limit} files")

    # Configuração S3
    s3_client = boto3.client('s3')
    s3_bucket = settings.s3_bucket
    s3_prefix_path = "raw/day=20231101/"
    base_url = settings.source_url + "/2023/11/01/"

    # Limpar arquivos existentes no S3
    logger.info(f"Cleaning existing files in s3://{s3_bucket}/{s3_prefix_path}")
    try:
        # Listar todos os objetos no prefixo
        paginator = s3_client.get_paginator('list_objects_v2')
        objects_to_delete = []
        for page in paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix_path):
            if 'Contents' in page:
                objects_to_delete.extend([{'Key': obj['Key']} for obj in page['Contents']])

        # Deletar objetos em lotes de 1000 (limite do S3)
        batch_size = 1000
        if objects_to_delete:
            total_objects = len(objects_to_delete)
            logger.info(f"Deleting {total_objects} existing files")

            for i in range(0, total_objects, batch_size):
                batch = objects_to_delete[i:i + batch_size]
                s3_client.delete_objects(
                    Bucket=s3_bucket,
                    Delete={'Objects': batch}
                )
                logger.info(f"Deleted batch of {len(batch)} files ({i + len(batch)}/{total_objects})")

            logger.info("All existing files deleted successfully")
    except Exception as e:
        logger.error(f"Error cleaning S3 bucket: {str(e)}")
        raise

    # Criar semáforo para limitar conexões simultâneas
    sem = asyncio.Semaphore(10)
    logger.info("Created semaphore with limit of 10 concurrent connections")

    async def download_and_upload_file(client: httpx.AsyncClient, i: int) -> None:
        async with sem:
            try:
                file_name = str(i).zfill(6) + 'Z.json.gz'
                file_url = f"{base_url}/{file_name}"
                s3_key = f"{s3_prefix_path}{file_name}"


                logger.debug(f"Downloading {file_url}")
                response = await client.get(file_url, timeout=30)

                if response.status_code == 200:
                    try:

                        s3_client.upload_fileobj(BytesIO(response.content), s3_bucket, s3_key)
                        # Upload para S3
                        logger.info(f"Successfully uploaded {file_name} to s3://{s3_bucket}/{s3_key}")

                    except Exception as e:
                        logger.error(f"Error processing {file_url}: {str(e)}")
                else:
                    logger.warning(f"Failed to download {file_url}. Status code: {response.status_code}")

            except httpx.TimeoutException:
                logger.error(f"Timeout downloading {file_url}")
            except Exception as e:
                logger.error(f"Unexpected error processing {file_url}: {str(e)}")

    # Gerar índices de tempo
    indices = []
    downloads_counter = 0

    def format_time(h, m, s):
        return h * 10000 + m * 100 + s

    hours, minutes, seconds = 0, 0, 0
    while downloads_counter < file_limit and hours < 24:
        current_index = format_time(hours, minutes, seconds)
        indices.append(current_index)
        downloads_counter += 1

        # Incrementa o tempo em intervalos de 5 segundos
        seconds += 5
        if seconds >= 60:
            seconds = 0
            minutes += 1
            if minutes >= 60:
                minutes = 0
                hours += 1

    logger.info(f"Generated {len(indices)} time indices from {indices[0]:06d}Z to {indices[-1]:06d}Z")

    # Executar downloads em paralelo
    async with httpx.AsyncClient() as client:
        tasks = [download_and_upload_file(client, i) for i in indices]
        await asyncio.gather(*tasks)

    end_time = time.time()
    execution_time = end_time - time_start
    logger.info(f"Download and upload completed. Execution time: {execution_time} seconds")

    return "OK"

@s4.post("/aircraft/prepare")
async def prepare_data() -> str:
    """Obtain the data from AWS s3 and store it in the local `prepared` directory
    as done in s2.
    """
    time_start = time.time()
    logger.info("Starting data preparation from S3")

    # Configuração S3
    s3_client = boto3.client('s3')
    s3_bucket = settings.s3_bucket
    raw_prefix = "raw/day=20231101/"
    prepared_prefix = "prepared/day=20231101/"

    # Limpar arquivos preparados existentes
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        objects_to_delete = []
        for page in paginator.paginate(Bucket=s3_bucket, Prefix=prepared_prefix):
            if 'Contents' in page:
                objects_to_delete.extend([{'Key': obj['Key']} for obj in page['Contents']])
        if objects_to_delete:
            logger.info(f"Deleting {len(objects_to_delete)} existing prepared files")
            s3_client.delete_objects(
                Bucket=s3_bucket,
                Delete={'Objects': objects_to_delete}
            )
    except Exception as e:
        logger.error(f"Error cleaning prepared files: {str(e)}")
        raise

    async def process_file(s3_key: str) -> pd.DataFrame:
        try:
            # Download do arquivo do S3
            response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
            json_content = response['Body'].read()
            data = json.loads(json_content)

            df_raw = pd.DataFrame([{
                'raw_data': json.dumps(data),
                'timestamp': data['now'],
            }])

            # Processa todas as linhas de uma vez usando apply
            def process_row(row):
                data = json.loads(row['raw_data'])
                return pd.Series({
                    'timestamp': data['now'],
                    'aircraft_data': data['aircraft']
                })

            # Processamento vetorizado
            processed = df_raw.apply(process_row, axis=1)

            # Explode aircraft_data para criar um DataFrame com todas as aeronaves
            df = pd.DataFrame(processed['aircraft_data'].explode().tolist())
            df['timestamp'] = processed['timestamp'].iloc[0]  # broadcast timestamp

            return df
        except Exception as e:
            logger.error(f"Error processing file {s3_key}: {str(e)}")
            raise

    # Lista todos os arquivos raw
    raw_files = []
    for page in paginator.paginate(Bucket=s3_bucket, Prefix=raw_prefix):
        if 'Contents' in page:
            raw_files.extend([obj['Key'] for obj in page['Contents']])

    # Processa arquivos em chunks para otimizar memória
    chunk_size = 50
    all_aircraft_info = []
    all_positions = []
    all_stats = []

    for i in range(0, len(raw_files), chunk_size):
        chunk_files = raw_files[i:i + chunk_size]
        tasks = [process_file(file) for file in chunk_files]
        chunk_dfs = await asyncio.gather(*tasks)

        # Combina os resultados do chunk
        df = pd.concat(chunk_dfs, ignore_index=True)

        # Extrai informações das aeronaves (vetorizado)
        aircraft_info = df[['hex', 'r', 't']].drop_duplicates()
        aircraft_info = aircraft_info.sort_values('hex')
        aircraft_info.columns = ['icao', 'registration', 'type']
        all_aircraft_info.append(aircraft_info)

        # Extrai posições (vetorizado)
        positions = df[['hex', 'timestamp', 'lat', 'lon']].copy().sort_values(['hex', 'timestamp'])
        positions.columns = ['icao', 'timestamp', 'lat', 'lon']
        all_positions.append(positions)

        # Calcula estatísticas (vetorizado)
        df['emergency'] = df['emergency'].fillna('none')
        df['had_emergency'] = (df['emergency'] != 'none')
        df['alt_baro'] = pd.to_numeric(df['alt_baro'], errors='coerce')
        df['gs'] = pd.to_numeric(df['gs'], errors='coerce')

        stats = df.groupby('hex').agg({
            'alt_baro': 'max',
            'gs': 'max',
            'had_emergency': 'max'
        }).reset_index()
        stats.columns = ['icao', 'max_altitude_baro', 'max_ground_speed', 'had_emergency']
        all_stats.append(stats)

        logger.info(f"Processed chunk {i//chunk_size + 1} of {(len(raw_files) + chunk_size - 1)//chunk_size}")

    # Função auxiliar para upload
    async def upload_dataframe(df: pd.DataFrame, filename: str):
        buffer = BytesIO()
        await asyncio.to_thread(df.to_csv, buffer, index=False)
        buffer.seek(0)
        s3_key = f"{prepared_prefix}{filename}"
        await asyncio.to_thread(
            s3_client.upload_fileobj,
            buffer,
            s3_bucket,
            s3_key
        )
        logger.info(f"Uploaded {filename} to s3://{s3_bucket}/{s3_key}")

    # Upload dos resultados finais
    await asyncio.gather(
        upload_dataframe(
            pd.concat(all_aircraft_info, ignore_index=True)
            .drop_duplicates()
            .sort_values('icao'),
            'aircraft_info.csv'
        ),
        upload_dataframe(
            pd.concat(all_positions, ignore_index=True)
            .sort_values(['icao', 'timestamp']),
            'positions.csv'
        ),
        upload_dataframe(
            pd.concat(all_stats, ignore_index=True)
            .groupby('icao')
            .agg({
                'max_altitude_baro': 'max',
                'max_ground_speed': 'max',
                'had_emergency': 'max'
            })
            .reset_index(),
            'statistics.csv'
        )
    )

    time_end = time.time()
    logger.info(f"Execution time: {time_end - time_start}")

    return "OK"
