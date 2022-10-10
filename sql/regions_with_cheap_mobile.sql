select distinct city_name
from fact_trip ft 
join dim_datasource dd on dd.id = ft.datasource_id 
join dim_region dr on dr.id = ft.region_id 
where dd.datasource = 'cheap_mobile'