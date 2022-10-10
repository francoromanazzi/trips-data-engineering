-- Obtain the weekly average number of trips for an area, defined by a bounding box (given by coordinates) or by a region.

select * from weekly_avg_trips_by_region('Prague') 

select * from weekly_avg_trips_by_box(
	ROW(
		ROW(10.0, 10.0)::coord,
		ROW(10.0, 60.0)::coord,
		ROW(60.0, 10.0)::coord,
		ROW(60.0, 60.0)::coord
	)::bounding_box
);