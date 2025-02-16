import asyncio
import glob
import json
import logging
import os
import time
from datetime import datetime
from typing import Annotated

import httpx
import pandas as pd
from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings

settings = Settings()

s1 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s1",
    tags=["s1"],
)

# Configuração do logger apenas para console
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.handlers = [console_handler]  # Substitui todos os handlers existentes

@s1.post("/aircraft/download")
async def download_data(
    file_limit: Annotated[
        int,
        Query(
            ...,
            description="""
    Limits the number of files to download.
    """,
        ),
    ] = 100,
) -> str:
    """Downloads the `file_limit` files AS IS inside the folder data/20231101"""
    time_start = time.time()
    logger.info(f"Starting download of {file_limit} files")
    # directory configuration
    download_dir = os.path.join(settings.raw_dir, "day=20231101")
    base_url = settings.source_url + "/2023/11/01/"
    os.makedirs(download_dir, exist_ok=True)

    # delete old files
    for f in glob.glob(os.path.join(download_dir, "*.parquet")):
        os.remove(f)

    # Criar semáforo para limitar conexões simultâneas
    sem = asyncio.Semaphore(10)  # Limita a 10 conexões simultâneas
    logger.info("Created semaphore with limit of 10 concurrent connections")

    async def download_single_file(client: httpx.AsyncClient, i: int) -> None:
        async with sem:  # Usar semáforo para controlar concorrência
            try:
                file_name = str(i).zfill(6) + 'Z.json.gz'
                file_url = f"{base_url}/{file_name}"
                parquet_path = os.path.join(download_dir, file_name.replace('.json.gz', '.parquet'))
                logger.debug(f"Acquired semaphore for downloading {file_url}")
                response = await client.get(file_url, timeout=30)
                if response.status_code == 200:
                    try:
                        logger.debug(f"Successfully downloaded {file_url}")

                        # Criar DataFrame e salvar como Parquet
                        df = pd.DataFrame([{
                            'raw_data': json.dumps(response.json()),
                            'timestamp': time.time(),
                        }])
                        df.to_parquet(parquet_path)
                        logger.info(f"Successfully processed and saved {file_name}")

                    except httpx.DecodingError as e:
                        logger.error(f"Decompression error for {file_url}: {str(e)}")
                        logger.error(f"Response headers: {response.headers}")
                        logger.error(f"Response size: {len(response.content)} bytes")
                    except Exception as e:
                        logger.error(f"Error processing {file_url}: {str(e)}")
                else:
                    logger.warning(f"Failed to download {file_url}. Status code: {response.status_code}")

            except httpx.TimeoutException:
                logger.error(f"Timeout downloading {file_url}")
            except Exception as e:
                logger.error(f"Unexpected error downloading {file_url}: {str(e)}")
            finally:
                logger.debug(f"Released semaphore for {file_url}")

    # Gerar índices
    indices = []
    i = 0
    downloads_counter = 0

    while downloads_counter < file_limit:
        if i > 235955:  # Último horário possível (23:59:55)
            break
        indices.append(i)
        downloads_counter += 1
        seconds = i % 100
        minutes = (i // 100) % 100
        hours = i // 10000
        if seconds == 55:
            if minutes == 59:
                i = (hours + 1) * 10000
            else:
                i = (i // 100 + 1) * 100
        else:
            i += 5
    async with httpx.AsyncClient() as client:
        tasks = [download_single_file(client, i) for i in indices]
        await asyncio.gather(*tasks)
    end_time = time.time()
    execution_time = end_time - time_start
    logger.info(f"Download completed. Execution time: {execution_time} seconds")
    logger.info(f"Successfully downloaded {len(glob.glob(os.path.join(download_dir, '*.parquet')))} files")

    return 'OK'


@s1.post("/aircraft/prepare")
async def prepare_data() -> str:
    """Prepara os dados para análise eficiente"""
    time_start = time.time()
    raw_dir = os.path.join(settings.raw_dir, "day=20231101")
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")

    os.makedirs(prepared_dir, exist_ok=True)
    for f in glob.glob(os.path.join(prepared_dir, "*.csv")):
        os.remove(f)

    async def process_file(file: str) -> pd.DataFrame:
        # Lê o arquivo Parquet em uma thread separada
        df_raw = await asyncio.to_thread(pd.read_parquet, file)

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

    # Processa arquivos em chunks para otimizar memória
    chunk_size = 50  # Ajuste baseado na memória disponível
    files = glob.glob(os.path.join(raw_dir, "*.parquet"))

    all_aircraft_info = []
    all_positions = []
    all_stats = []

    for i in range(0, len(files), chunk_size):
        chunk_files = files[i:i + chunk_size]
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

        logger.info(f"Processed chunk {i//chunk_size + 1} of {(len(files) + chunk_size - 1)//chunk_size}")

    # Combina e salva os resultados finais
    await asyncio.gather(
        asyncio.to_thread(
            pd.concat(all_aircraft_info, ignore_index=True)
            .drop_duplicates()
            .sort_values('icao')
            .to_csv, os.path.join(prepared_dir, 'aircraft_info.csv'), index=False
        ),
        asyncio.to_thread(
            pd.concat(all_positions, ignore_index=True)
            .sort_values(['icao', 'timestamp'])
            .to_csv, os.path.join(prepared_dir, 'positions.csv'), index=False
        ),
        asyncio.to_thread(
            pd.concat(all_stats, ignore_index=True)
            .groupby('icao')
            .agg({
                'max_altitude_baro': 'max',
                'max_ground_speed': 'max',
                'had_emergency': 'max'
            })
            .reset_index()
            .to_csv, os.path.join(prepared_dir, 'statistics.csv'), index=False
        )
    )

    time_end = time.time()
    logger.info(f"Execution time: {time_end - time_start}")

    return "OK"

@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc
    """
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    info_file = os.path.join(prepared_dir, 'aircraft_info.csv')
    aircraft_info = pd.read_csv(info_file)
    start_idx = page * num_results
    end_idx = start_idx + num_results
    paginated_info = aircraft_info.iloc[start_idx:end_idx]

    return paginated_info.to_dict('records')

@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list.
    """
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    positions_file = os.path.join(prepared_dir, 'positions.csv')
    positions = pd.read_csv(positions_file)
    aircraft_positions = positions[positions['icao'] == icao]
    if len(aircraft_positions) == 0:
        return []  # Retorna lista vazia se não encontrar o avião
    # Ordenar por timestamp e converter para lista de dicionários
    aircraft_positions = aircraft_positions.sort_values('timestamp')
    start_idx = page * num_results
    end_idx = start_idx + num_results
    paginated_positions = aircraft_positions.iloc[start_idx:end_idx]

    return paginated_positions.to_dict('records')

@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed
    * had_emergency
    """
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    stats_file = os.path.join(prepared_dir, 'statistics.csv')
    stats = pd.read_csv(stats_file)
    aircraft_stats = stats[stats['icao'] == icao]
    stats_dict = aircraft_stats.iloc[0].to_dict()
    stats_dict['had_emergency'] = bool(stats_dict['had_emergency'])
    stats_dict['max_altitude_baro'] = float(stats_dict['max_altitude_baro'])
    stats_dict['max_ground_speed'] = float(stats_dict['max_ground_speed'])

    return stats_dict
