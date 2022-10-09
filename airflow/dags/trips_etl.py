import airflow
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator

import pandas as pd
from datetime import datetime, timedelta
from io import StringIO


default_args = {
    "owner": "Airflow", 
    "start_date" : datetime(2022,10,9),
    "retries" : 1,
    "retry_delay": timedelta(minutes=5)
}


def _process_data(ti):
    data = ti.xcom_pull(task_ids = 'extract_trips') # Extract the csv data from XCom

    df = pd.read_csv(StringIO(data))

    # Remove duplicates
    df = df.drop_duplicates(subset=['origin_coord', 'destination_coord', 'datetime'])
    print(df)

    # Create dimensional model
    #df_region = df[['region']].drop_duplicates()
    #df_region['id'] = df.index

    #print(df_region)

    # Export DataFrame to CSV
    df.to_csv('/tmp/processed_data.csv', index=None)


with DAG('trips_pipeline', default_args=default_args, schedule_interval=None, catchup=False) as dag:

    # Task #1 - Extract Data
    extract_data = SimpleHttpOperator(
            task_id = 'extract_trips',
            http_conn_id='gdrive',
            method='GET',
            endpoint="uc?id=14JcOSJAWqKOUNyadVZDPm7FplA7XYhrU",
            response_filter=lambda response: response.text,
            log_response=True
    )

    # Task #2 - Process data
    transform_data = PythonOperator(
        task_id = 'transform_trips',
        python_callable=_process_data

    )

    # Task #3 - Truncate DW tables
    truncate_dw_tables = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id='postgres',
        sql='''
            TRUNCATE TABLE dim_region;
        '''
    )

    # Dependencies
    extract_data >> transform_data >> truncate_dw_tables
