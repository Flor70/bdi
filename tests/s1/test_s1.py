from fastapi.testclient import TestClient


class TestS1Student:
    """
    As you code it's always important to ensure that your code reflects
    the business requisites you have.
    The optimal way to do so is via tests.
    Use this class to create functions to test your application.

    For more information on the library used, search `pytest` in your preferred search engine.
    """

    def test_first(self, client: TestClient) -> None:
        # Implement tests if you want
        with client as client:
            response = client.post("/api/s1/aircraft/download?file_limit=1")
            assert True

    def test_download_time_format(self, client: TestClient) -> None:
        """Testa se os arquivos são baixados com o formato de tempo correto"""
        with client as client:
            response = client.post("/api/s1/aircraft/download?file_limit=10")
            assert response.status_code == 200
            
            # Verifica se os arquivos foram salvos com nomes no formato correto
            import glob
            import os
            from bdi_api.settings import Settings
            
            settings = Settings()
            files = glob.glob(os.path.join(settings.raw_dir, "day=20231101", "*.parquet"))
            
            for f in files:
                # Extrai o timestamp do nome do arquivo (removendo .parquet)
                time_str = os.path.basename(f).replace('.parquet', '')
                # Verifica formato HHMMSS
                hours = int(time_str[:2])
                minutes = int(time_str[2:4])
                seconds = int(time_str[4:6])
                
                assert 0 <= hours <= 23, f"Horas inválidas: {hours}"
                assert 0 <= minutes <= 59, f"Minutos inválidos: {minutes}"
                assert seconds % 5 == 0 and seconds < 60, f"Segundos inválidos: {seconds}"

    def test_pagination_aircraft_list(self, client: TestClient) -> None:
        """Testa a paginação do endpoint list_aircraft"""
        with client as client:
            # Primeiro, garante que temos dados preparados
            client.post("/api/s1/aircraft/download?file_limit=10")
            client.post("/api/s1/aircraft/prepare")
            
            # Testa primeira página
            response = client.get("/api/s1/aircraft/?page=0&num_results=5")
            assert response.status_code == 200
            first_page = response.json()
            assert len(first_page) <= 5
            
            # Testa segunda página
            response = client.get("/api/s1/aircraft/?page=1&num_results=5")
            second_page = response.json()
            
            # Verifica que as páginas são diferentes
            if len(second_page) > 0:
                assert first_page[0]['icao'] != second_page[0]['icao']

    def test_pagination_positions(self, client: TestClient) -> None:
        """Testa a paginação do endpoint get_aircraft_position"""
        with client as client:
            # Primeiro, garante que temos dados preparados
            client.post("/api/s1/aircraft/download?file_limit=10")
            client.post("/api/s1/aircraft/prepare")
            
            # Obtém um ICAO válido
            response = client.get("/api/s1/aircraft")
            icao = response.json()[0]['icao']
            
            # Testa paginação das posições
            response = client.get(f"/api/s1/aircraft/{icao}/positions?page=0&num_results=5")
            assert response.status_code == 200
            positions = response.json()
            assert len(positions) <= 5
            
            if len(positions) > 0:
                # Verifica ordenação por timestamp
                timestamps = [p['timestamp'] for p in positions]
                assert timestamps == sorted(timestamps)

    def test_statistics_calculation(self, client: TestClient) -> None:
        """Testa se as estatísticas estão sendo calculadas corretamente"""
        with client as client:
            # Primeiro, garante que temos dados preparados
            client.post("/api/s1/aircraft/download?file_limit=10")
            client.post("/api/s1/aircraft/prepare")
            
            # Obtém um ICAO válido
            response = client.get("/api/s1/aircraft")
            icao = response.json()[0]['icao']
            
            # Obtém estatísticas
            response = client.get(f"/api/s1/aircraft/{icao}/stats")
            assert response.status_code == 200
            stats = response.json()
            
            # Verifica tipos de dados, permitindo None
            for field in ['max_altitude_baro', 'max_ground_speed']:
                assert stats[field] is None or isinstance(stats[field], (int, float)), \
                    f"Campo {field} deve ser None ou numérico"
                if stats[field] is not None:
                    assert stats[field] >= 0, f"Campo {field} deve ser não-negativo quando presente"
            
            # had_emergency é sempre booleano, mesmo que False
            assert isinstance(stats['had_emergency'], bool)

    def test_data_consistency(self, client: TestClient) -> None:
        """Testa a consistência dos dados entre os endpoints"""
        with client as client:
            # Primeiro, garante que temos dados preparados
            client.post("/api/s1/aircraft/download?file_limit=10")
            client.post("/api/s1/aircraft/prepare")
            
            # Obtém lista de aeronaves
            response = client.get("/api/s1/aircraft")
            aircraft_list = response.json()
            
            # Para cada aeronave, verifica consistência dos dados
            for aircraft in aircraft_list[:3]:  # Testa as 3 primeiras para não demorar muito
                icao = aircraft['icao']
                
                # Verifica posições
                response = client.get(f"/api/s1/aircraft/{icao}/positions")
                positions = response.json()
                if len(positions) > 0:
                    assert all(p['icao'] == icao for p in positions)
                
                # Verifica estatísticas
                response = client.get(f"/api/s1/aircraft/{icao}/stats")
                stats = response.json()
                assert 'icao' in stats
                assert stats['icao'] == icao


class TestItCanBeEvaluated:
    """
    Those tests are just to be sure I can evaluate your exercise.
    Don't modify anything from here!

    Make sure all those tests pass with `poetry run pytest` or it will be a 0!
    """

    def test_download(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s1/aircraft/download?file_limit=1")
            assert not response.is_error, "Error at the download endpoint"

    def test_prepare(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s1/aircraft/prepare")
            assert not response.is_error, "Error at the prepare endpoint"

    def test_aircraft(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s1/aircraft")
            assert not response.is_error, "Error at the aircraft endpoint"
            r = response.json()
            assert isinstance(r, list), "Result is not a list"
            assert len(r) > 0, "Result is empty"
            for field in ["icao", "registration", "type"]:
                assert field in r[0], f"Missing '{field}' field."

    def test_positions(self, client: TestClient) -> None:
        icao = "06a0af"
        with client as client:
            response = client.get(f"/api/s1/aircraft/{icao}/positions")
            assert not response.is_error, "Error at the positions endpoint"
            r = response.json()
            assert isinstance(r, list), "Result is not a list"
            assert len(r) > 0, "Result is empty"
            for field in ["timestamp", "lat", "lon"]:
                assert field in r[0], f"Missing '{field}' field."

    def test_stats(self, client: TestClient) -> None:
        icao = "06a0af"
        with client as client:
            response = client.get(f"/api/s1/aircraft/{icao}/stats")
            assert not response.is_error, "Error at the positions endpoint"
            r = response.json()
            for field in ["max_altitude_baro", "max_ground_speed", "had_emergency"]:
                assert field in r, f"Missing '{field}' field."
