import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

# Adjust imports based on your project structure
from bdi_api.app import app  # Assuming your FastAPI app instance is here
from bdi_api.s8.database import Base, get_db
from bdi_api.s8.database import Aircraft, FuelConsumption, Position  # Import models

# --- Test Database Setup ---
SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"  # Use in-memory SQLite for tests

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},  # Needed for SQLite
    poolclass=StaticPool,  # Use StaticPool for testing
)
TestingSessionLocal = sessionmaker(
    autocommit=False, autoflush=False, bind=engine)

# Create tables in the in-memory database before tests run
Base.metadata.create_all(bind=engine)

# --- Dependency Override ---


def override_get_db():
    """Overrides the get_db dependency to use the test database session."""
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()


# Apply the dependency override to the FastAPI app
app.dependency_overrides[get_db] = override_get_db

# --- Test Client ---
client = TestClient(app)

# --- Test Data Fixtures ---


@pytest.fixture(scope="function", autouse=True)
def setup_database():
    """ Cleans and sets up the database with test data before each test function. """
    # Clean tables before each test
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

    db = TestingSessionLocal()
    try:
        # Add Test Aircraft
        test_aircraft1 = Aircraft(icao="a835af", registration="N628TS",
                                  type="GLF6", manufacturer="GULFSTREAM", model="G650ER")
        test_aircraft2 = Aircraft(icao="c0ffee", registration="TEST1",
                                  type="B738", manufacturer="BOEING", model="737-800")
        test_aircraft3 = Aircraft(
            icao="decade", registration="TEST2", type="A320", manufacturer="AIRBUS", model="A320")
        db.add_all([test_aircraft1, test_aircraft2, test_aircraft3])

        # Add Test Fuel Consumption
        fuel_glf6 = FuelConsumption(
            aircraft_type="GLF6", name="Gulfstream G650", galph=503)
        fuel_b738 = FuelConsumption(
            aircraft_type="B738", name="Boeing 737-800", galph=850)
        # No fuel data for A320 intentionally
        db.add_all([fuel_glf6, fuel_b738])

        # Add Test Positions (Example for 'a835af' on 2023-11-01)
        # 720 records = 1 hour (720 * 5s = 3600s)
        ts_nov_1_2023 = 1698811200  # 2023-11-01 04:00:00 UTC (example start)
        positions = []
        for i in range(720):  # 1 hour of data
            positions.append(
                Position(icao="a835af", timestamp=ts_nov_1_2023 + i * 5, lat=40.0, lon=-74.0))
        # Add one position for c0ffee on same day
        positions.append(
            Position(icao="c0ffee", timestamp=ts_nov_1_2023 + 10 * 5, lat=50.0, lon=0.0))

        db.add_all(positions)
        db.commit()
    finally:
        db.close()

    yield  # Test runs here

    # Teardown can happen here if needed, but drop_all handles cleanup


# --- Test Functions ---

def test_list_aircraft_default():
    """ Test the /aircraft/ endpoint with default parameters. """
    response = client.get("/api/s8/aircraft/")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 3  # We added 3 aircraft
    # Check if aircraft are sorted by icao
    assert data[0]["icao"] == "a835af"
    assert data[1]["icao"] == "c0ffee"
    assert data[2]["icao"] == "decade"
    # Check structure of the first item
    assert data[0]["registration"] == "N628TS"
    assert data[0]["type"] == "GLF6"


def test_list_aircraft_pagination():
    """ Test pagination for the /aircraft/ endpoint. """
    # Request first page with 1 result
    response = client.get("/api/s8/aircraft/?num_results=1&page=0")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["icao"] == "a835af"

    # Request second page with 1 result
    response = client.get("/api/s8/aircraft/?num_results=1&page=1")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["icao"] == "c0ffee"

    # Request page with no results
    response = client.get("/api/s8/aircraft/?num_results=1&page=5")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 0


def test_get_aircraft_co2_success():
    """ Test CO2 calculation for an aircraft with data. """
    response = client.get("/api/s8/aircraft/a835af/co2?day=2023-11-01")
    assert response.status_code == 200
    data = response.json()
    assert data["icao"] == "a835af"
    assert data["hours_flown"] == 1.0  # 720 records * 5s / 3600s/hr
    assert data["co2"] is not None
    # Manual calculation:
    # fuel_gal = 1.0 hr * 503 gal/hr = 503 gal
    # fuel_kg = 503 * 3.04 = 1529.12 kg
    # co2_tons = (1529.12 * 3.15) / 907.185 = 5.319...
    assert abs(data["co2"] - 5.32) < 0.01  # Check calculation approximately


def test_get_aircraft_co2_no_fuel_data():
    """ Test CO2 calculation for an aircraft missing fuel consumption data. """
    response = client.get(
        "/api/s8/aircraft/decade/co2?day=2023-11-01")  # A320 has no fuel data in fixture
    assert response.status_code == 200
    data = response.json()
    assert data["icao"] == "decade"
    assert data["hours_flown"] == 0.0  # No position data added for this icao
    assert data["co2"] is None


def test_get_aircraft_co2_no_flight_data():
    """ Test CO2 calculation for a day with no flight data. """
    response = client.get(
        "/api/s8/aircraft/a835af/co2?day=2023-11-02")  # No position data for this day
    assert response.status_code == 200
    data = response.json()
    assert data["icao"] == "a835af"
    assert data["hours_flown"] == 0.0
    assert data["co2"] is None  # Cannot calculate CO2 with 0 hours flown


def test_get_aircraft_co2_invalid_date():
    """ Test CO2 endpoint with an invalid date format. """
    response = client.get(
        "/api/s8/aircraft/a835af/co2?day=20231101")  # Incorrect format
    assert response.status_code == 400  # Expecting Bad Request
    assert "Invalid date format" in response.json()["detail"]


def test_get_aircraft_co2_not_found_icao():
    """ Test CO2 endpoint with an ICAO not in the database. """
    # Note: Current implementation doesn't explicitly check if ICAO exists before counting positions.
    # It might return 0 hours_flown and None co2, which might be acceptable.
    # A 404 might be preferable if the aircraft ICAO itself is not found in the Aircraft table.
    # Adding a check for ICAO existence in the endpoint might be a future improvement.
    response = client.get("/api/s8/aircraft/abcdef/co2?day=2023-11-01")
    assert response.status_code == 200  # Currently returns 200
    data = response.json()
    assert data["icao"] == "abcdef"
    assert data["hours_flown"] == 0.0
    assert data["co2"] is None
