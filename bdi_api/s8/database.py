import os
from sqlalchemy import (
    create_engine, Column, String, Integer, Float, Boolean, ForeignKey, Index
)
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
from sqlalchemy.pool import QueuePool
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database connection details from environment variables
DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "aircraftdb")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- Database Connection Setup ---

# Configure connection pool suitable for potentially limited RDS resources
engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=5,  # Keep pool size small for smaller instances
    max_overflow=2,
    pool_timeout=30,  # seconds
    pool_recycle=1800,  # seconds (recycle connections every 30 mins)
    echo=False  # Set to True for debugging SQL statements
)

# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for declarative models
Base = declarative_base()

# --- SQLAlchemy Models ---


class Aircraft(Base):
    """ Core aircraft information model. """
    __tablename__ = "aircraft"

    # Indexed for API lookups
    icao = Column(String, primary_key=True, index=True)
    registration = Column(String, nullable=True)
    # Indexed for joining with fuel consumption
    type = Column(String, nullable=True, index=True)
    owner = Column(String, nullable=True)
    manufacturer = Column(String, nullable=True)
    model = Column(String, nullable=True)

    # Relationships (optional, but can be useful)
    positions = relationship("Position", back_populates="aircraft")
    statistics = relationship("Statistics", back_populates="aircraft")


class FuelConsumption(Base):
    """ Fuel consumption rates for different aircraft types. """
    __tablename__ = "fuel_consumption"

    # Indexed for quick lookup in CO2 calc
    aircraft_type = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=True)
    galph = Column(Float, nullable=True)  # Gallons per hour
    category = Column(String, nullable=True)
    source = Column(String, nullable=True)


class Position(Base):
    """ Aircraft position data from ADS-B Exchange. """
    __tablename__ = "positions"

    # Using a combination of icao and timestamp for potential PK,
    # but a dedicated UUID or sequence might be better for very large scale.
    # For simplicity now, let's assume icao + timestamp is unique enough for the exercise context,
    # or rely on a separate loader process to handle uniqueness if needed.
    # Adding a simple Integer PK for now.
    id = Column(Integer, primary_key=True, autoincrement=True)
    icao = Column(String, ForeignKey("aircraft.icao"),
                  index=True)  # FK to Aircraft
    timestamp = Column(Integer, index=True)  # Unix timestamp
    lat = Column(Float, nullable=True)
    lon = Column(Float, nullable=True)
    # Example: Using barometric altitude if available
    altitude_baro = Column(Float, nullable=True)
    ground_speed = Column(Float, nullable=True)
    # Add other relevant fields from readsb-hist as needed

    aircraft = relationship("Aircraft", back_populates="positions")

    # Composite index for efficient lookup by aircraft and time (essential for CO2 calc)
    __table_args__ = (
        Index('idx_position_icao_timestamp', 'icao', 'timestamp'),
    )


class Statistics(Base):
    """ Pre-calculated statistics for aircraft, potentially per day. """
    __tablename__ = "statistics"

    # Composite primary key: unique identifier for an aircraft on a specific day
    icao = Column(String, ForeignKey("aircraft.icao"), primary_key=True)
    day = Column(String, primary_key=True)  # Format 'YYYY-MM-DD'

    max_altitude_baro = Column(Float, nullable=True)
    max_ground_speed = Column(Float, nullable=True)
    had_emergency = Column(Boolean, nullable=True)
    # Crucial for CO2 endpoint optimization
    hours_flown = Column(Float, nullable=True)

    aircraft = relationship("Aircraft", back_populates="statistics")

    # Index on the composite PK is implicit, but an explicit index on icao might be useful too
    __table_args__ = (
        Index('idx_statistics_icao_day', 'icao', 'day'),
    )

# --- Helper Functions ---


def get_db():
    """ Dependency injector for FastAPI routes to get a database session. """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def create_db_and_tables():
    """ Creates database tables based on the defined models. """
    print("Creating database tables...")
    try:
        Base.metadata.create_all(bind=engine)
        print("Tables created successfully (if they didn't exist).")
    except Exception as e:
        print(f"Error creating tables: {e}")


if __name__ == "__main__":
    # This allows running the script directly to create tables
    print("Running table creation...")
    create_db_and_tables()
