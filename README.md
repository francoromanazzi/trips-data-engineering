# trips-data-engineering
> Sample ETL project using Python, Airflow, Postgres & Docker for a trips dataset

## Requirements
* Docker v20.10+

## Usage
##### Clone the repo
```
git clone https://github.com/francoromanazzi/trips-data-engineering.git
```
##### Create .env file like this:
```
POSTGRES_DB=airflow
POSTGRES_DW_DB=trips
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
```
##### Run docker compose and wait a few minutes:
```
docker-compose up -d
```
##### Go to airflow UI:
```
http://localhost:8080
user: airflow
pass: airflow
```
##### Run DAG:
```
Click 'trips_pipeline' -> Hit the run button
```

## Architecture

![Architecture](/docs/architecture.png?raw=true 'Architecture')

## Data model

![Datamodel](/docs/entity-relationship.png?raw=true 'ERD')
* The postgres Data Warehouse uses a Star Schema because of its simplicity.
* The fact table is what is known as a 'Factless fact table' because it has no measurements inside of it.
It only contains references to the dimensions

## Sample raw data

![Raw](/docs/raw-data.png?raw=true 'raw')


## Repository structure
* ```/airflow/dags``` Python ETL code using Airflow
* ```/sql``` Various sql queries for data analysis
* ```init-db.sql``` Database object definitions
* ```docker-compose.yaml``` Docker container definitions

## Regarding scalability
In order to make the automated process truly scalable, it would need to run in a distributed environment, which would make it much harder to develop and test locally (without incurring cloud provider costs).
Some cloud services that I would use would be, for example:
* ```AWS Glue or Databricks``` for writing distributed Python jobs using Spark without worrying about provisioning the cluster
* ```AWS Redshift, Google BigQuery or Snowflake``` for a highly optimized Data Warehouse
* ```AWS Step Functions or AWS MWAA``` for pipeline orchestration
