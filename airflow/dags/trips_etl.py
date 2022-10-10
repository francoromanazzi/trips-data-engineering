import airflow
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

import pandas as pd
from datetime import datetime, timedelta
from io import StringIO


default_args = {
    "owner": "Airflow", 
    "start_date" : datetime(2022,10,9),
    "retries" : 1,
    "retry_delay": timedelta(minutes=5)
}


def _transform_data(ti):
    data = ti.xcom_pull(task_ids = 'extract_trips') # Extract the csv data from XCom

    df = pd.read_csv(StringIO(data))

    # Remove duplicates
    df = df.drop_duplicates(subset=['origin_coord', 'destination_coord', 'datetime'])
    print(df)

    # Create dimensional model
    # 1. dim_region
    df_region = df[['region']].drop_duplicates()
    df_region = df_region.reset_index()
    df_region = df_region.rename(columns={"region": "city_name", "index":"id"})
    df_region['id'] = df_region.index
    print(df_region)
    df_region.to_csv('/tmp/dim_region.csv', index=None)

    df = df.merge(df_region, left_on=['region'], right_on=['city_name'])
    df = df.drop(['region', 'city_name'], axis=1)
    df = df.rename(columns={"id": "region_id"})
    print(df)

    # 2. dim_datetime
    df_datetime = df[['datetime']].drop_duplicates()
    df_datetime = df_datetime.reset_index()
    df_datetime = df_datetime.rename(columns={"index":"id"})
    df_datetime['id'] = df_datetime.index
    print(df_datetime)
    df_datetime.to_csv('/tmp/dim_datetime.csv', index=None)

    df = df.merge(df_datetime, on=['datetime'])
    df = df.drop('datetime', axis=1)
    df = df.rename(columns={"id": "datetime_id"})
    print(df)

    # 3. dim_datasource
    df_datasource = df[['datasource']].drop_duplicates()
    df_datasource = df_datasource.reset_index()
    df_datasource = df_datasource.rename(columns={"index":"id"})
    df_datasource['id'] = df_datasource.index
    print(df_datasource)
    df_datasource.to_csv('/tmp/dim_datasource.csv', index=None)

    df = df.merge(df_datasource, on=['datasource'])
    df = df.drop('datasource', axis=1)
    df = df.rename(columns={"id": "datasource_id"})
    print(df)

    # 4. dim_coordinate
    df_coordinate = pd.concat([df[['origin_coord']].rename(columns={"origin_coord":"coordinate"}), df[['destination_coord']].rename(columns={"destination_coord":"coordinate"})])
    df_coordinate = df_coordinate.drop_duplicates()
    df_coordinate[['x', 'y']] = df_coordinate['coordinate'].apply(lambda x: pd.Series([x.split(' ')[1][1:], x.split(' ')[2][:-1]]))
    print(df_coordinate)
    df_coordinate = df_coordinate.reset_index()
    df_coordinate = df_coordinate.rename(columns={"index":"id"})
    df_coordinate['id'] = df_coordinate.index
    print(df_coordinate)

    df = df.merge(df_coordinate, left_on=['origin_coord'], right_on=['coordinate'])
    df = df.drop(['origin_coord', 'coordinate', 'x', 'y'], axis=1)
    df = df.rename(columns={"id": "origin_coord_id"})

    df = df.merge(df_coordinate, left_on=['destination_coord'], right_on=['coordinate'])
    df = df.drop(['destination_coord', 'coordinate', 'x', 'y'], axis=1)
    df = df.rename(columns={"id": "destination_coord_id"})

    df_coordinate = df_coordinate.drop('coordinate', axis=1)
    df_coordinate.to_csv('/tmp/dim_coordinate.csv', index=None)

    print(df)

    # 5. fact_trip
    df.to_csv('/tmp/fact_trip.csv', index=None)


def _load_data():
    # Connect to the Postgres connection
    hook = PostgresHook(postgres_conn_id = 'postgres')

    # Insert the data from the CSV files into the postgres database
    for table in ['dim_region', 'dim_datetime', 'dim_datasource', 'dim_coordinate', 'fact_trip']:
        hook.copy_expert(
            sql = f"COPY {table} FROM stdin WITH (FORMAT CSV, HEADER, DELIMITER ',')",
            filename=f'/tmp/{table}.csv'
        )


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

    # Task #2 - Transform data
    transform_data = PythonOperator(
        task_id = 'transform_trips',
        python_callable=_transform_data

    )

    # Task #3 - Truncate postgres tables
    truncate_tables = PostgresOperator(
        task_id = 'truncate_tables',
        postgres_conn_id='postgres',
        sql='''
            TRUNCATE TABLE dim_region CASCADE;
            TRUNCATE TABLE dim_datetime CASCADE;
            TRUNCATE TABLE dim_datasource CASCADE;
            TRUNCATE TABLE dim_coordinate CASCADE;
            TRUNCATE TABLE fact_trip CASCADE;
        '''
    )

    # Task #4 - Load data into Postgres
    load_data = PythonOperator(
        task_id = 'load_trips',
        python_callable=_load_data

    )

    # Dependencies
    extract_data >> transform_data >> truncate_tables >> load_data
