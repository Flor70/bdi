import os
from typing import Annotated

from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings

import requests
import os
import glob
import json
import pandas as pd

settings = Settings()

s1 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s1",
    tags=["s1"],
)


@s1.post("/aircraft/download")
def download_data(
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
    """Downloads the `file_limit` files AS IS inside the folder data/20231101

    data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to configure it in the `pyproject.toml` file
    so it can be installed using `poetry update`.


    TIP: always clean the download folder before writing again to avoid having old files.
    """
    download_dir = os.path.join(settings.raw_dir, "day=20231101")
    base_url = settings.source_url + "/2023/11/01/"
    # TODO Implement download

    os.makedirs(download_dir, exist_ok=True)

    for f in glob.glob(os.path.join(download_dir, "*.gz")):
        os.remove(f)

    for i in range(0, file_limit*5, 5):  
        file_name = str(i).zfill(6) + 'Z.json.gz'
        file_url = f"{base_url}/{file_name}"

        response = requests.get(file_url) # , params=params, headers=headers

        file_path = os.path.join(download_dir, file_name)
        print(f'i: {i} ')
        print(f'file name: {file_name} ')


        if response.status_code == 200:  
            print('response = 200')
            with open(file_path, 'w') as f:
                json.dump(response.json(), f)
                print('arquivo criado')

    return "OK"


@s1.post("/aircraft/prepare")
def prepare_data() -> str:
    """Prepare the data in the way you think it's better for the analysis.

    * data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    * documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to configure it in the `pyproject.toml` file
    so it can be installed using `poetry update`.

    TIP: always clean the prepared folder before writing again to avoid having old files.

    Keep in mind that we are downloading a lot of small files, and some libraries might not work well with this!
    """
    # TODO




    return "OK"


@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc
    """
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    info_file = os.path.join(prepared_dir, 'aircraft_info.csv')
    
    aircraft_info = pd.read_csv(info_file)
    
    return aircraft_info.to_dict('records')    


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
    return aircraft_positions.to_dict('records')


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
     prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    stats_file = os.path.join(prepared_dir, 'statistics.csv')
    
    if not os.path.exists(stats_file):
        raise HTTPException(status_code=400, detail="Data not prepared. Call /aircraft/prepare first")
    
    stats = pd.read_csv(stats_file)
    aircraft_stats = stats[stats['icao'] == icao]
    
    if len(aircraft_stats) == 0:
        raise HTTPException(status_code=404, detail=f"Aircraft {icao} not found")
    
    # Converter para dicionário e garantir tipos corretos
    stats_dict = aircraft_stats.iloc[0].to_dict()
    stats_dict['had_emergency'] = bool(stats_dict['had_emergency'])  # garantir que é boolean
    stats_dict['max_altitude_baro'] = float(stats_dict['max_altitude_baro'])  # garantir que é float
    stats_dict['max_ground_speed'] = float(stats_dict['max_ground_speed'])  # garantir que é float
    
    return stats_dict
    
    if len(aircraft_stats) == 0:
        raise HTTPException(status_code=404, detail=f"Aircraft {icao} not found")
    
    # Converter para dicionário e garantir tipos corretos
    stats_dict = aircraft_stats.iloc[0].to_dict()
    stats_dict['had_emergency'] = bool(stats_dict['had_emergency'])  # garantir que é boolean
    stats_dict['max_altitude_baro'] = float(stats_dict['max_altitude_baro'])  # garantir que é float
    stats_dict['max_ground_speed'] = float(stats_dict['max_ground_speed'])  # garantir que é float
    
    return stats_dict
