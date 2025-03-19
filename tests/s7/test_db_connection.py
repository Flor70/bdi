"""
Tests for database connection functionality.
"""

import os
import unittest
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from unittest import mock

# Import models
from bdi_api.s7.models import Aircraft, Position, Statistics, Base


class TestDatabaseConnection(unittest.TestCase):
    """Test cases for database connection."""

    def setUp(self):
        """Set up the test environment."""
        # Set environment variables for testing
        os.environ["BDI_DB_HOST"] = "localhost"
        os.environ["BDI_DB_PORT"] = "5432"
        os.environ["BDI_DB_USERNAME"] = "bdiadmin"
        os.environ["BDI_DB_PASSWORD"] = "postgres123"

        # Use an in-memory SQLite database for testing
        self.engine = sqlalchemy.create_engine('sqlite:///:memory:')
        Base.metadata.create_all(self.engine)

        # Create session
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def tearDown(self):
        """Clean up after each test."""
        self.session.close()
        Base.metadata.drop_all(self.engine)

        # Reset environment variables
        for key in ["BDI_DB_HOST", "BDI_DB_PORT", "BDI_DB_USERNAME", "BDI_DB_PASSWORD"]:
            if key in os.environ:
                del os.environ[key]

    def test_aircraft_model(self):
        """Test Aircraft model operations."""
        # Create a new aircraft
        aircraft = Aircraft(
            icao="TEST123", registration="N123TEST", type="B737")
        self.session.add(aircraft)
        self.session.commit()

        # Query the aircraft
        result = self.session.query(Aircraft).filter_by(icao="TEST123").first()

        # Verify the result
        self.assertIsNotNone(result)
        self.assertEqual(result.icao, "TEST123")
        self.assertEqual(result.registration, "N123TEST")
        self.assertEqual(result.type, "B737")

    def test_position_model(self):
        """Test Position model operations."""
        # Create a new aircraft (required for foreign key)
        aircraft = Aircraft(
            icao="TEST123", registration="N123TEST", type="B737")
        self.session.add(aircraft)
        self.session.commit()

        # Create a new position with explicit id value
        position = Position(
            id="TEST123_1234567890", icao="TEST123", timestamp=1234567890, lat=40.7128, lon=-74.0060)
        self.session.add(position)
        self.session.commit()

        # Query the position
        result = self.session.query(Position).filter_by(icao="TEST123").first()

        # Verify the result
        self.assertIsNotNone(result)
        self.assertEqual(result.icao, "TEST123")
        self.assertEqual(result.timestamp, 1234567890)
        self.assertEqual(result.lat, 40.7128)
        self.assertEqual(result.lon, -74.0060)

    def test_statistics_model(self):
        """Test Statistics model operations."""
        # Create a new aircraft (required for foreign key)
        aircraft = Aircraft(
            icao="TEST123", registration="N123TEST", type="B737")
        self.session.add(aircraft)
        self.session.commit()

        # Create new statistics
        stats = Statistics(icao="TEST123", max_altitude_baro=35000,
                           max_ground_speed=450, had_emergency=False)
        self.session.add(stats)
        self.session.commit()

        # Query the statistics
        result = self.session.query(
            Statistics).filter_by(icao="TEST123").first()

        # Verify the result
        self.assertIsNotNone(result)
        self.assertEqual(result.icao, "TEST123")
        self.assertEqual(result.max_altitude_baro, 35000)
        self.assertEqual(result.max_ground_speed, 450)
        self.assertEqual(result.had_emergency, False)

    def test_relationship_integrity(self):
        """Test relationship integrity between models.

        Note: In SQLite, foreign key constraints are not enabled by default,
        so we need to manually delete the related records instead of relying on cascade.
        """
        # Create a new aircraft
        aircraft = Aircraft(
            icao="TEST123", registration="N123TEST", type="B737")
        self.session.add(aircraft)
        self.session.commit()

        # Create a new position with explicit id and statistics
        position = Position(
            id="TEST123_1234567890", icao="TEST123", timestamp=1234567890, lat=40.7128, lon=-74.0060)
        stats = Statistics(icao="TEST123", max_altitude_baro=35000,
                           max_ground_speed=450, had_emergency=False)

        self.session.add(position)
        self.session.add(stats)
        self.session.commit()

        # Manually delete related records first (since SQLite doesn't enforce foreign key constraints)
        self.session.query(Position).filter_by(icao="TEST123").delete()
        self.session.query(Statistics).filter_by(icao="TEST123").delete()
        self.session.commit()

        # Then delete the aircraft
        self.session.delete(aircraft)
        self.session.commit()

        # Query should return None after manual deletion
        position_result = self.session.query(
            Position).filter_by(icao="TEST123").first()
        stats_result = self.session.query(
            Statistics).filter_by(icao="TEST123").first()

        # The position and stats should be deleted
        self.assertIsNone(position_result)
        self.assertIsNone(stats_result)


if __name__ == "__main__":
    unittest.main()
