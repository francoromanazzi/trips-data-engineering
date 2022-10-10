CREATE DATABASE trips;

CREATE TABLE dim_region (
	id INT PRIMARY KEY,
	city_name VARCHAR(255)
);

CREATE TABLE dim_datetime (
	id INT PRIMARY KEY,
	datetime timestamp
);

CREATE TABLE dim_datasource (
	id INT PRIMARY KEY,
	datasource VARCHAR(255)
);

CREATE TABLE dim_coordinate (
	id INT PRIMARY KEY,
	x NUMERIC(32, 28),
    y NUMERIC(32, 28)
);

CREATE TABLE fact_trip (
    region_id INT REFERENCES dim_region,
    datetime_id INT REFERENCES dim_datetime,
    datasource_id INT REFERENCES dim_datasource,
    origin_coord_id INT REFERENCES dim_coordinate,
    destination_coord_id INT REFERENCES dim_coordinate,
    PRIMARY KEY (
        region_id,
        datetime_id,
        datasource_id,
        origin_coord_id,
        destination_coord_id
    )
);
