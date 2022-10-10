with trips_with_latest_datasource_of_region as (
	select *, first_value(dds.datasource) over (
		partition by ft.region_id 
		order by ddt.datetime desc
	) latest_datasource_of_region
	from fact_trip ft 
	join dim_datetime ddt on ddt.id = ft.datetime_id 
	join dim_datasource dds on dds.id = ft.datasource_id
)
select city_name, latest_datasource_of_region
from trips_with_latest_datasource_of_region trips
join dim_region dr on dr.id = trips.region_id
group by city_name, latest_datasource_of_region
order by count(*) desc
limit 2