"""
Tests for S3 access functionality.
"""

import os
import unittest
import json
from unittest import mock
import boto3
from botocore.exceptions import ClientError


class TestS3Access(unittest.TestCase):
    """Test cases for S3 access."""

    def setUp(self):
        """Set up the test environment."""
        # Set environment variables for testing
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ["BDI_S3_BUCKET"] = "bdi-aircraft-test"

        # Patch S3 client to avoid actual S3 connection
        self.s3_patcher = mock.patch("boto3.client")
        self.mock_s3_client = self.s3_patcher.start()

        # Setup mock S3 client behavior
        self.mock_s3 = mock.MagicMock()
        self.mock_s3_client.return_value = self.mock_s3

        # Sample S3 data
        self.sample_aircraft_data = [
            {
                "icao": "test123",
                "registration": "N123TEST",
                "type": "B737",
                "positions": [
                    {"timestamp": 1234567890, "lat": 40.7128, "lon": -74.0060}
                ],
                "stats": {
                    "max_altitude_baro": 35000,
                    "max_ground_speed": 450,
                    "had_emergency": False
                }
            }
        ]

    def tearDown(self):
        """Clean up after each test."""
        # Stop patcher
        self.s3_patcher.stop()

        # Reset environment variables
        for key in ["AWS_REGION", "BDI_S3_BUCKET"]:
            if key in os.environ:
                del os.environ[key]

    def test_s3_list_objects(self):
        """Test listing objects in S3 bucket."""
        # Mock list_objects_v2 to return sample objects
        self.mock_s3.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "folder1/aircraft1.json"},
                {"Key": "folder1/aircraft2.json"},
                {"Key": "folder2/aircraft3.json"}
            ]
        }

        # Get S3 client
        s3_client = boto3.client('s3')

        # List objects
        response = s3_client.list_objects_v2(
            Bucket=os.environ["BDI_S3_BUCKET"],
            Prefix="folder1/"
        )

        # Verify response
        self.assertIn("Contents", response)
        self.assertEqual(len(response["Contents"]), 3)

        # Verify mock was called correctly
        self.mock_s3.list_objects_v2.assert_called_once_with(
            Bucket=os.environ["BDI_S3_BUCKET"],
            Prefix="folder1/"
        )

    def test_s3_get_object(self):
        """Test getting an object from S3 bucket."""
        # Mock get_object to return sample aircraft data
        self.mock_s3.get_object.return_value = {
            "Body": mock.MagicMock(
                read=mock.MagicMock(
                    return_value=json.dumps(self.sample_aircraft_data).encode()
                )
            )
        }

        # Get S3 client
        s3_client = boto3.client('s3')

        # Get object
        response = s3_client.get_object(
            Bucket=os.environ["BDI_S3_BUCKET"],
            Key="folder1/aircraft1.json"
        )

        # Read and parse the data
        body = response["Body"].read()
        data = json.loads(body.decode())

        # Verify data
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["icao"], "test123")
        self.assertEqual(data[0]["registration"], "N123TEST")

        # Verify mock was called correctly
        self.mock_s3.get_object.assert_called_once_with(
            Bucket=os.environ["BDI_S3_BUCKET"],
            Key="folder1/aircraft1.json"
        )

    def test_s3_bucket_not_found(self):
        """Test behavior when bucket is not found."""
        # Mock list_objects_v2 to raise ClientError (bucket not found)
        self.mock_s3.list_objects_v2.side_effect = ClientError(
            {
                'Error': {
                    'Code': 'NoSuchBucket',
                    'Message': 'The specified bucket does not exist'
                }
            },
            'ListObjectsV2'
        )

        # Get S3 client
        s3_client = boto3.client('s3')

        # Attempt to list objects (should raise exception)
        with self.assertRaises(ClientError) as context:
            s3_client.list_objects_v2(
                Bucket="non-existent-bucket",
                Prefix="folder1/"
            )

        # Verify exception
        self.assertEqual(
            context.exception.response['Error']['Code'], 'NoSuchBucket')

    def test_s3_object_not_found(self):
        """Test behavior when object is not found."""
        # Mock get_object to raise ClientError (object not found)
        self.mock_s3.get_object.side_effect = ClientError(
            {
                'Error': {
                    'Code': 'NoSuchKey',
                    'Message': 'The specified key does not exist.'
                }
            },
            'GetObject'
        )

        # Get S3 client
        s3_client = boto3.client('s3')

        # Attempt to get object (should raise exception)
        with self.assertRaises(ClientError) as context:
            s3_client.get_object(
                Bucket=os.environ["BDI_S3_BUCKET"],
                Key="non-existent-key.json"
            )

        # Verify exception
        self.assertEqual(
            context.exception.response['Error']['Code'], 'NoSuchKey')


if __name__ == "__main__":
    unittest.main()
