from sqlalchemy import Column, String, Float, Boolean, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from bdi_api.settings import DBCredentials
import os

# Try to get credentials from environment variables
try:
    # Try to use environment variables first
    db_credentials = DBCredentials()
except Exception as e:
    # If it fails, use hardcoded values for local development
    print(f"Error loading DBCredentials: {e}")
    print("Using default values for local connection...")

    # Temporary class to simulate DBCredentials
    class LocalDBCredentials:
        def __init__(self):
            self.host = "localhost"
            self.port = 5432
            self.username = "bdiadmin"
            self.password = "postgres123"

    db_credentials = LocalDBCredentials()

# Print credentials for debugging
print(
    f"Connecting to database: {db_credentials.host}:{db_credentials.port} as {db_credentials.username}")

# Database configuration
DATABASE_URL = f"postgresql://{db_credentials.username}:{db_credentials.password}@{db_credentials.host}:{db_credentials.port}/aircraft_db"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class Aircraft(Base):
    __tablename__ = "aircraft"

    icao = Column(String, primary_key=True, index=True)
    registration = Column(String, nullable=True)
    type = Column(String, nullable=True)


class Position(Base):
    __tablename__ = "positions"

    id = Column(String, primary_key=True)  # Combination of icao and timestamp
    icao = Column(String, index=True)
    timestamp = Column(Float)
    lat = Column(Float)
    lon = Column(Float)


class Statistics(Base):
    __tablename__ = "statistics"

    icao = Column(String, primary_key=True, index=True)
    max_altitude_baro = Column(Float)
    max_ground_speed = Column(Float)
    had_emergency = Column(Boolean)


def create_tables():
    """Create tables in the database if they don't exist."""
    Base.metadata.create_all(bind=engine)


def get_db():
    """Provide a database session."""
    db = SessionLocal()
    try:
        return db
    finally:
        db.close()
