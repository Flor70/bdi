# S8 Exercise: Airflow ETL and FastAPI

## Project Overview

This project implements an end-to-end data pipeline for processing aircraft flight data and serving it via an API, fulfilling the requirements of the S8 exercise. It utilizes:

*   **Apache Airflow (via Astronomer):** For orchestrating ETL (Extract, Transform, Load) workflows.
*   **AWS S3:** As a data lake with bronze, silver, and gold layers.
*   **AWS RDS (PostgreSQL):** As the final data warehouse storing processed data for the API.
*   **FastAPI:** To provide API endpoints for accessing the processed aircraft and CO2 emission data.

The project consists of three main Airflow DAGs and a FastAPI application:

1.  **`readsb_hist_processing` DAG:** Downloads historical flight data (`.json.gz` files) for the first day of each month from ADS-B Exchange, processes it through S3 layers (bronze, silver, gold), and loads final data into RDS.
2.  **`aircraft_fuel_consumption_dag`:** Downloads and processes aircraft fuel consumption rates from a JSON source, loading data into RDS.
3.  **`aircraft_db_dag`:** Downloads and processes a basic aircraft database (`.json.gz`), loading data into RDS.
4.  **FastAPI Application (`exercise.py`):** Provides endpoints to query the aircraft information and calculated CO2 emissions stored in RDS.

## Architecture

(A visual architecture diagram would ideally go here or in a separate `docs/` folder).

The basic data flow is:
1.  **Sources:** ADS-B Exchange website, GitHub JSON file, ADS-B Exchange DB download.
2.  **Airflow DAGs (Extract/Transform/Load):**
    *   Fetch data from sources.
    *   Store raw data in S3 Bronze layer (`s3://<your-bucket>/bronze/...`).
    *   Process and clean data, store in S3 Silver layer (`s3://<your-bucket>/silver/...`, typically Parquet).
    *   Aggregate/prepare data for API, store in S3 Gold layer (`s3://<your-bucket>/gold/...`, typically Parquet).
    *   Load Gold layer data into RDS PostgreSQL tables (`aircraft`, `aircraft_co2`, `positions`).
3.  **RDS Database:** Stores the final, queryable datasets.
4.  **FastAPI:** Queries the RDS database to serve requests via HTTP endpoints.

## Setup Instructions

### Prerequisites

*   Git
*   Docker Desktop (running)
*   Python (3.9+ recommended)
*   Poetry (`pip install poetry`)
*   Astronomer CLI (`brew install astro` or other installation method)
*   AWS Account with credentials configured locally (e.g., via `~/.aws/credentials` or environment variables)
*   Access to an AWS S3 bucket and an RDS PostgreSQL instance.

### Steps

1.  **Clone Repository:**
    ```bash
    git clone <your-repo-url>
    cd <your-repo-directory>
    ```
2.  **Install Dependencies:**
    ```bash
    poetry install
    ```
3.  **Start Airflow Environment:**
    ```bash
    astro dev start
    ```
    This will build the Docker image and start Airflow containers (Webserver, Scheduler, Postgres, Triggerer).

## Configuration

Before running the DAGs, you need to configure the following in the Airflow UI (http://localhost:8080, default login: `admin`/`admin`):

### Airflow Connections

Navigate to **Admin -> Connections**. Add/Edit the following:

1.  **`aws_default`**
    *   **Conn Type:** `Amazon Web Services`
    *   **AWS Access Key ID:** Your AWS Access Key ID
    *   **AWS Secret Access Key:** Your AWS Secret Access Key
    *   **Region Name:** (Optional) Your default AWS region (e.g., `us-east-1`)
2.  **`postgres_default`**
    *   **Conn Type:** `Postgres`
    *   **Host:** Your RDS instance endpoint URL
    *   **Schema:** Your database name within the RDS instance
    *   **Login:** Your database master username
    *   **Password:** Your database master password
    *   **Port:** Your database port (usually `5432`)

### Airflow Variables

Navigate to **Admin -> Variables**. Add the following:

1.  **`s3_bucket`**
    *   **Key:** `s3_bucket`
    *   **Val:** The name of your S3 bucket (e.g., `bdi-s8-data-lake-5103`)
2.  **`readsb_url`** (Optional, defaults provided in DAG)
    *   **Key:** `readsb_url`
    *   **Val:** `https://samples.adsbexchange.com/readsb-hist` (or other if needed)
3.  **`max_files`** (Optional, defaults provided in DAG)
    *   **Key:** `max_files`
    *   **Val:** `100` (or other limit for readsb processing)

## Running the DAGs

1.  Access the Airflow UI at http://localhost:8080.
2.  Unpause the following DAGs by toggling the switch next to their names:
    *   `readsb_hist_processing` (Schedule: `@monthly`, processes 1st of the month)
    *   `aircraft_db_dag` (Schedule: `None`)
    *   `aircraft_fuel_consumption_dag` (Schedule: `None`)
3.  You can manually trigger each DAG by clicking the "Play" button next to its name.
    *   It's recommended to run `aircraft_db_dag` and `aircraft_fuel_consumption_dag` first, as `readsb_hist_processing` might implicitly depend on the tables they create/populate during its gold layer processing (although the template focuses on `positions` and `aircraft_co2`).
    *   Trigger `readsb_hist_processing` for a specific logical date if needed (e.g., the first of a past month).
4.  Monitor the DAG runs in the Grid or Graph view for successful completion. Check logs for details.

## API Usage

The FastAPI application runs within the Airflow webserver container (or can be run separately if needed). Assuming it's accessible via the Airflow environment:

1.  **List Aircraft:** Retrieves a paginated list of aircraft.
    ```bash
    # Get first page (default 10 results)
    curl http://localhost:8080/api/s8/aircraft/
    
    # Get page 2 with 5 results per page
    curl http://localhost:8080/api/s8/aircraft/?page=2&num_results=5
    ```
2.  **Get CO2 Emissions:** Calculates estimated CO2 emissions for a specific aircraft on a given day.
    ```bash
    # Replace {icao} and {YYYY-MM-DD}
    curl http://localhost:8080/api/s8/aircraft/{icao}/co2?day={YYYY-MM-DD} 

    # Example:
    curl http://localhost:8080/api/s8/aircraft/A8AABC/co2?day=2025-04-01 
    ```

## Testing

To run the automated API tests:

1.  Ensure the Airflow environment is running (`astro dev start`).
2.  Ensure the necessary data has been loaded into RDS by running the DAGs successfully.
3.  Execute the tests using pytest within the Poetry environment:
    ```bash
    poetry run pytest tests/s8/test_exercise.py
    ```
    All tests should pass.
