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

create function weekly_avg_trips_by_region (region VARCHAR(255))
returns float
as $$
SELECT COUNT(*) * 7.0 / ((MAX(dd.datetime)::date - MIN(dd.datetime)::date) + 1) weekly_avg_trips
from fact_trip ft
join dim_region dr on dr.id = ft.region_id
join dim_datetime dd on dd.id = ft.datetime_id
where dr.city_name = region;
$$
language sql;

create type coord as (
	x NUMERIC(32, 28),
    y NUMERIC(32, 28)
);

create type bounding_box as (
	p1 coord,
	p2 coord,
	p3 coord,
	p4 coord
);

create function is_coord_inside_box (point coord, bbox bounding_box)
returns boolean
as $func$
	select exists(
		SELECT * FROM (
	        select
		        least   ((bbox).p1.x, (bbox).p2.x, (bbox).p3.x, (bbox).p4.x) AS minlat,
		        least   ((bbox).p1.y, (bbox).p2.y, (bbox).p3.y, (bbox).p4.y) AS minlon,
		        greatest((bbox).p1.x, (bbox).p2.x, (bbox).p3.x, (bbox).p4.x) AS maxlat,
		        GREATEST((bbox).p1.y, (bbox).p2.y, (bbox).p3.y, (bbox).p4.y) AS maxlon
	) rect
	WHERE point.x >= rect.minlat AND point.x < rect.maxlat
	AND point.y >= minlon AND point.y < rect.maxlon
	)
$func$
language sql;

create function weekly_avg_trips_by_box (bbox bounding_box)
returns float
as $$
SELECT COUNT(*) * 7.0 / ((MAX(dd.datetime)::date - MIN(dd.datetime)::date) + 1) weekly_avg_trips
from fact_trip ft
join dim_coordinate dco on dco.id = ft.origin_coord_id 
join dim_coordinate dcd on dcd.id = ft.destination_coord_id 
join dim_datetime dd on dd.id = ft.datetime_id
where is_coord_inside_box(ROW(dco.x, dco.y)::coord, bbox)
and is_coord_inside_box(ROW(dcd.x, dcd.y)::coord, bbox);
$$
language sql;
