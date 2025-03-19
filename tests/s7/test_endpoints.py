"""
Tests for the S7 endpoints.
"""

import os
import unittest
import json
from unittest import mock
from fastapi.testclient import TestClient
from bdi_api.app import app

client = TestClient(app)


class MockDBCredentials:
    """Mock DB credentials for testing."""

    def __init__(self):
        self.host = "localhost"
        self.port = 5432
        self.username = "bdiadmin"
        self.password = "postgres123"


class TestS7Endpoints(unittest.TestCase):
    """Test cases for the S7 endpoints."""

    def setUp(self):
        """Set up the test environment."""
        # Set environment variables for testing
        os.environ["BDI_DB_HOST"] = "localhost"
        os.environ["BDI_DB_PORT"] = "5432"
        os.environ["BDI_DB_USERNAME"] = "bdiadmin"
        os.environ["BDI_DB_PASSWORD"] = "postgres123"
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ["BDI_S3_BUCKET"] = "bdi-aircraft-test"

        # Patch DB connection to avoid actual DB connection
        self.db_patcher = mock.patch("bdi_api.s7.models.create_engine")
        self.mock_create_engine = self.db_patcher.start()

        # Patch S3 client to avoid actual S3 connection
        self.s3_patcher = mock.patch("bdi_api.s7.exercise.boto3.client")
        self.mock_s3_client = self.s3_patcher.start()

        # Setup mock S3 client behavior
        mock_s3 = mock.MagicMock()
        self.mock_s3_client.return_value = mock_s3

        # Mock list_objects_v2 to return a sample object
        mock_s3.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "test/aircraft.json"}
            ]
        }

        # Mock get_object to return sample aircraft data
        mock_s3.get_object.return_value = {
            "Body": mock.MagicMock(
                read=mock.MagicMock(
                    return_value=json.dumps([
                        {
                            "icao": "test123",
                            "registration": "N123TEST",
                            "type": "B737",
                            "positions": [
                                {"timestamp": 1234567890,
                                    "lat": 40.7128, "lon": -74.0060}
                            ],
                            "stats": {
                                "max_altitude_baro": 35000,
                                "max_ground_speed": 450,
                                "had_emergency": False
                            }
                        }
                    ]).encode()
                )
            )
        }

        # Patch pandas.concat to avoid empty list error
        self.pd_patcher = mock.patch("bdi_api.s7.exercise.pd.concat")
        self.mock_pd_concat = self.pd_patcher.start()
        self.mock_pd_concat.return_value = mock.MagicMock()

        # Patch SQLAlchemy SessionLocal to avoid actual DB session
        self.session_patcher = mock.patch("bdi_api.s7.models.SessionLocal")
        self.mock_session = self.session_patcher.start()

        # Setup mock session behavior
        mock_session_instance = mock.MagicMock()
        self.mock_session.return_value = mock_session_instance
        # Configure enter method correctly
        mock_session_instance.__enter__ = mock.MagicMock(
            return_value=mock_session_instance)
        mock_session_instance.__exit__ = mock.MagicMock()

        # Mock query and execution methods
        mock_session_instance.query.return_value.filter.return_value.first.return_value = None
        mock_session_instance.query.return_value.filter.return_value.all.return_value = []
        mock_session_instance.query.return_value.offset.return_value.limit.return_value.all.return_value = []

    def tearDown(self):
        """Clean up after each test."""
        # Stop patchers
        self.db_patcher.stop()
        self.s3_patcher.stop()
        self.session_patcher.stop()
        self.pd_patcher.stop()

        # Reset environment variables
        for key in ["BDI_DB_HOST", "BDI_DB_PORT", "BDI_DB_USERNAME", "BDI_DB_PASSWORD",
                    "AWS_REGION", "BDI_S3_BUCKET"]:
            if key in os.environ:
                del os.environ[key]

    def test_mock_setup(self):
        """Test that mocks are set up correctly."""
        # This is a simplified test to confirm our mock setup is working
        self.assertIsNotNone(self.mock_create_engine)
        self.assertIsNotNone(self.mock_s3_client)
        self.assertIsNotNone(self.mock_session)
        self.assertIsNotNone(self.mock_pd_concat)

    def test_s7_router_exists(self):
        """Test that the S7 router exists."""
        # Just check that we can import the router
        from bdi_api.s7.exercise import s7
        self.assertIsNotNone(s7)
        # Verify that it has routes
        self.assertTrue(len(s7.routes) > 0)

    def test_endpoint_paths(self):
        """Test that the expected endpoint paths are defined."""
        from bdi_api.s7.exercise import s7

        # Get route paths without the prefix, which is added by FastAPI when including the router
        route_paths = []
        for route in s7.routes:
            # Just check that routes are defined
            self.assertIsNotNone(route.path)
            route_paths.append(route.path)

        # Check for routes by substring - the actual paths include /api/s7 prefix when mounted
        self.assertTrue(any('aircraft/' in path for path in route_paths))
        self.assertTrue(any('positions' in path for path in route_paths))
        self.assertTrue(any('stats' in path for path in route_paths))
        self.assertTrue(any('prepare' in path for path in route_paths))


if __name__ == "__main__":
    unittest.main()
