# Airflow DAGs for S8 Exercise

This directory should contain the DAG files needed for the S8 exercise. Below are the required DAGs and their purpose.

## Required DAGs

### 1. readsb_hist_dag.py

This DAG should download and process the readsb-hist data from ADS-B Exchange.

Key requirements:
- Limit to 100 files per day
- Handle one day at a time (first day of each month)
- Separate steps for download and prepare
- Ensure idempotency

### 2. aircraft_fuel_consumption_dag.py

This DAG should download and process the aircraft fuel consumption rates.

Source: [aircraft_type_fuel_consumption_rates.json](https://github.com/martsec/flight_co2_analysis/blob/main/data/aircraft_type_fuel_consumption_rates.json)

### 3. aircraft_db_dag.py

This DAG should download and process the Aircraft Database.

Source: [basic-ac-db.json.gz](http://downloads.adsbexchange.com/downloads/basic-ac-db.json.gz)

## Template Structure

Each DAG should follow a similar structure:

```python
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'dag_name',
    default_args=default_args,
    description='Description of the DAG',
    schedule_interval=None,  # or '@daily', '@weekly', etc.
    catchup=False,
)

# Define functions for tasks
def download_data(**context):
    # Implementation for downloading data
    pass

def process_data(**context):
    # Implementation for processing data
    pass

def load_to_database(**context):
    # Implementation for loading to database
    pass

# Define tasks
download_task = PythonOperator(
    task_id='download_data',
    python_callable=download_data,
    provide_context=True,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_database',
    python_callable=load_to_database,
    provide_context=True,
    dag=dag,
)

# Define dependencies
download_task >> process_task >> load_task
```

## Data Flow Architecture

The DAGs should follow this data flow pattern:

1. **Extract:** Download data from sources
2. **Load:** Store raw data in S3 (bronze layer)
3. **Transform (1st stage):** Process raw data into a more structured format (silver layer)
4. **Transform (2nd stage):** Create analysis-ready data (gold layer)
5. **Load to Database:** Store processed data in RDS for API consumption

## Integration with API

The data processed by these DAGs will be accessed by the FastAPI endpoints defined in `bdi_api/s8/exercise.py`. Ensure that:

1. The database structure matches what the API expects
2. The data is properly formatted for easy querying
3. Performance considerations are addressed (indexes, etc.)

## Important Considerations

1. **Idempotency:** Ensure that running a DAG multiple times doesn't result in duplicate data
2. **Error Handling:** Implement robust error handling to deal with network issues, data inconsistencies, etc.
3. **Logging:** Use Airflow's logging to track the progress and status of the DAGs
4. **Configuration:** Use environment variables or Airflow variables for configuration
5. **Testing:** Test your DAGs thoroughly before submission

## Example DAGs

You can refer to the Airflow documentation for example DAGs:
- [DAGs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html)
- [Python Operator](https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/python.html) 