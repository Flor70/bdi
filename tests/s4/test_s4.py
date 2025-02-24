import os
import json
import boto3
import pytest
import pandas as pd
from fastapi.testclient import TestClient
from moto import mock_s3
from io import BytesIO

from bdi_api.settings import Settings

settings = Settings()

@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    """Create mocked S3 client."""
    with mock_s3():
        s3 = boto3.client("s3", region_name="us-east-1")
        # Criar bucket
        s3.create_bucket(Bucket=settings.s3_bucket)
        
        # Criar alguns dados de teste
        test_data = pd.DataFrame([{
            'raw_data': json.dumps({
                'now': 1234567890,
                'aircraft': [{
                    'hex': 'abc123',
                    'r': 'TEST123',
                    't': 'B738',
                    'alt_baro': 30000,
                    'gs': 450,
                    'emergency': 'none',
                    'lat': 41.123,
                    'lon': 2.123
                }]
            }),
            'timestamp': 1234567890
        }])
        
        # Salvar como Parquet
        parquet_buffer = BytesIO()
        test_data.to_parquet(parquet_buffer)
        parquet_buffer.seek(0)
        
        # Upload para S3
        s3.put_object(
            Bucket=settings.s3_bucket,
            Key="raw/day=20231101/000000Z.parquet",
            Body=parquet_buffer.getvalue()
        )
        
        yield s3

class TestS4Student:
    """Testes para a implementação S4 que usa AWS S3"""

    def test_download_time_format(self, client: TestClient, s3_client) -> None:
        """Testa se os arquivos são baixados com o formato de tempo correto"""
        with client as client:
            response = client.post("/api/s4/aircraft/download?file_limit=10")
            assert response.status_code == 200

            # Lista arquivos no S3
            response = s3_client.list_objects_v2(
                Bucket=settings.s3_bucket,
                Prefix="raw/day=20231101/"
            )
            
            assert 'Contents' in response, "Nenhum arquivo foi criado no S3"
            files = response['Contents']
            
            for obj in files:
                # Extrai o timestamp do nome do arquivo
                file_name = os.path.basename(obj['Key'])
                if not file_name.endswith('.parquet'):
                    continue
                    
                time_str = file_name.replace('.parquet', '')
                
                # Verifica formato HHMMSS
                hours = int(time_str[:2])
                minutes = int(time_str[2:4])
                seconds = int(time_str[4:6])
                
                assert 0 <= hours <= 23, f"Horas inválidas: {hours}"
                assert 0 <= minutes <= 59, f"Minutos inválidos: {minutes}"
                assert seconds % 5 == 0 and seconds < 60, f"Segundos inválidos: {seconds}"

    def test_prepare_data_structure(self, client: TestClient, s3_client) -> None:
        """Testa se os arquivos preparados têm a estrutura correta"""
        with client as client:
            # Download e preparo dos dados
            response = client.post("/api/s4/aircraft/prepare")
            assert response.status_code == 200

            # Verifica se os arquivos foram criados no S3
            expected_files = ['aircraft_info.csv', 'positions.csv', 'statistics.csv']
            for filename in expected_files:
                response = s3_client.list_objects_v2(
                    Bucket=settings.s3_bucket,
                    Prefix=f"prepared/day=20231101/{filename}"
                )
                assert 'Contents' in response, f"Arquivo {filename} não foi criado no S3"

    def test_data_format(self, client: TestClient, s3_client) -> None:
        """Testa o formato dos dados nos arquivos preparados"""
        with client as client:
            # Prepara os dados
            response = client.post("/api/s4/aircraft/prepare")
            assert response.status_code == 200

            # Verifica aircraft_info.csv
            response = s3_client.get_object(
                Bucket=settings.s3_bucket,
                Key="prepared/day=20231101/aircraft_info.csv"
            )
            content = response['Body'].read().decode('utf-8').strip()
            assert content, "aircraft_info.csv está vazio"
            header = content.split('\n')[0]
            expected_columns = ['icao', 'registration', 'type']
            assert all(col in header for col in expected_columns), "Colunas incorretas em aircraft_info.csv"

            # Verifica positions.csv
            response = s3_client.get_object(
                Bucket=settings.s3_bucket,
                Key="prepared/day=20231101/positions.csv"
            )
            content = response['Body'].read().decode('utf-8').strip()
            assert content, "positions.csv está vazio"
            header = content.split('\n')[0]
            expected_columns = ['icao', 'timestamp', 'lat', 'lon']
            assert all(col in header for col in expected_columns), "Colunas incorretas em positions.csv"

            # Verifica statistics.csv
            response = s3_client.get_object(
                Bucket=settings.s3_bucket,
                Key="prepared/day=20231101/statistics.csv"
            )
            content = response['Body'].read().decode('utf-8').strip()
            assert content, "statistics.csv está vazio"
            header = content.split('\n')[0]
            expected_columns = ['icao', 'max_altitude_baro', 'max_ground_speed', 'had_emergency']
            assert all(col in header for col in expected_columns), "Colunas incorretas em statistics.csv"

    def test_s3_cleanup(self, client: TestClient, s3_client) -> None:
        """Testa se a limpeza dos arquivos funciona corretamente"""
        with client as client:
            # Primeira execução
            client.post("/api/s4/aircraft/download?file_limit=5")
            
            # Lista arquivos após primeira execução
            response = s3_client.list_objects_v2(
                Bucket=settings.s3_bucket,
                Prefix="raw/day=20231101/"
            )
            first_count = len(response.get('Contents', []))
            
            # Segunda execução
            client.post("/api/s4/aircraft/download?file_limit=3")
            
            # Lista arquivos após segunda execução
            response = s3_client.list_objects_v2(
                Bucket=settings.s3_bucket,
                Prefix="raw/day=20231101/"
            )
            second_count = len(response.get('Contents', []))
            
            # Verifica se os arquivos antigos foram limpos
            assert second_count == 3, "Arquivos antigos não foram limpos corretamente"


