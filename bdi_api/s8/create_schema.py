import sys
import os
import logging
from sqlalchemy import create_engine
from pydantic import ValidationError

# Add project root to path to allow imports like bdi_api.settings
project_root = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Now import necessary components
try:
    # Import necessary models and engine from S7, assuming S7 setup is available
    # If S7 models are not directly importable, you might need to copy/adapt them for S8
    from bdi_api.s7.models import Base, Aircraft, Position, Statistics
    # Use S8 settings if they define DB credentials, otherwise S7 models already configure engine via DBCredentials
    from bdi_api.settings import DBCredentials
except ImportError as e:
    print(f"Error importing modules: {e}")
    print("Ensure you are running this script from the project root directory or that the path is correct.")
    print("Also check if S7 models are correctly structured for import or need adaptation/copying to S8.")
    sys.exit(1)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_s8_tables():
    """Creates tables defined in S7 models within the S8 database."""
    logger.info(
        "Attempting to create database tables (aircraft, positions, statistics)...")
    try:
        # Load credentials using the correct prefix - with fallback for port error
        try:
            db_credentials = DBCredentials()
            logger.info(
                f"Loaded credentials for host: {db_credentials.host}, user: {db_credentials.username}, port: {db_credentials.port}")
        except ValidationError as e:
            # Check if the ONLY error is the port parsing
            is_only_port_error = len(e.errors()) == 1 and e.errors(
            )[0]['loc'][0] == 'port' and 'int_parsing' in str(e.errors()[0]['type'])
            if is_only_port_error:
                logger.warning(
                    f"Could not parse BDI_DB_PORT environment variable: {e}. Attempting to use default port 5432.")
                # Try loading again, explicitly setting port to the integer default
                # We still need host, username, password from env vars
                temp_creds = {'host': os.getenv('BDI_DB_HOST'),
                              'username': os.getenv('BDI_DB_USERNAME'),
                              'password': os.getenv('BDI_DB_PASSWORD'),
                              'port': 5432}  # Set explicitly to integer 5432
                if not all(v for k, v in temp_creds.items()):  # Check all required fields now
                    logger.error(
                        "Required DB credentials (HOST, USERNAME, PASSWORD) not found in environment variables.")
                    # Re-raise original error if other core creds are missing
                    raise ValueError(
                        "Missing required DB credentials (HOST, USERNAME, PASSWORD).") from e
                # If host/user/pass found, load with default port
                db_credentials = DBCredentials(**temp_creds)
                # Updated log
                logger.info(
                    f"Successfully loaded credentials using explicit port 5432 for host: {db_credentials.host}, user: {db_credentials.username}")
            else:
                # If other validation errors exist, raise the original error
                logger.error(
                    f"Failed to load DBCredentials due to multiple/other errors: {e}")
                raise e  # Re-raise the original validation error

        # --- Define the CORRECT Database Name for S8 ---
        db_name = "bdi_s8_db"  # As specified by user
        logger.info(f"Targeting database: {db_name}")

        # --- Construct the CORRECT Database URL ---
        database_url_s8 = (
            f"postgresql://{db_credentials.username}:{db_credentials.password}@"
            f"{db_credentials.host}:{db_credentials.port}/{db_name}"
        )

        # --- Create a NEW engine specifically for this script ---
        engine_s8 = create_engine(database_url_s8)
        logger.info(f"Using database engine for S8: {engine_s8}")

        # --- Drop the existing 'positions' table first (if it exists) ---
        try:
            logger.warning(
                f"Attempting to drop existing 'positions' table before recreating...")
            Base.metadata.drop_all(bind=engine_s8, tables=[Position.__table__])
            logger.info(
                "'positions' table dropped successfully (or did not exist).")
        except Exception as drop_err:
            logger.error(
                f"Error dropping 'positions' table (might not be critical if it didn't exist): {drop_err}")
            # Continue anyway, as create_all should handle it if it doesn't exist

        # --- Create tables using the CORRECT engine --- #
        # Base is imported from s7.models, containing table definitions
        Base.metadata.create_all(bind=engine_s8)
        logger.info(
            "Tables created successfully (or already exist) in database '{db_name}'.")

    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        logger.error(
            "Please check database connection details (env vars for DBCredentials), DB name, and permissions.")
        # Optionally re-raise or exit differently depending on desired behavior on error
        # raise


if __name__ == "__main__":
    # Optional: Add confirmation step - uncomment if desired
    # confirm = input("This will attempt to create tables in the configured S8 database. Proceed? (y/n): ")
    # if confirm.lower() == 'y':
    #     create_s8_tables()
    # else:
    #    logger.info("Table creation cancelled.")

    # Run directly without confirmation for automation
    create_s8_tables()
