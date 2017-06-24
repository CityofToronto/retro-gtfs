/*
create the trips table from python output
and  
generate geometries with azimuth for visualization
*/

-- read in the results from Python
DROP TABLE IF EXISTS rio2014_trips;
CREATE TABLE rio2014_trips (
	-- to be populated from csv vv
	trip_id integer PRIMARY KEY,
	block_id integer,
	vehicle_id varchar,
	route_id varchar,
	gps_uids integer[],
	-- to be populated from csv ^^
	direction_id varchar(35),
	-- comes later vv
	service_id integer,
	match_confidence real,
	match_geom geometry(LINESTRING,32723), -- map-matched route geometry
	orig_geom  geometry(LINESTRING,32723),	-- geometry of all points TODO kill
	clean_geom geometry(LINESTRING,32723), -- geometry of points used in map matching TODO kill
	problem varchar DEFAULT '',	-- description of any problems that arise
	ignore boolean DEFAULT FALSE,	-- ignore this vehicle during processing?
	azimuth real 			-- azimuth for pretty rendering

);
COPY rio2014_trips (trip_id, block_id, vehicle_id, route_id, gps_uids) FROM '/home/nate/rio/trips.csv' CSV HEADER;
CREATE INDEX rio2014_trips_idx ON rio2014_trips (trip_id);

-- populate geometry column with one big join/update
WITH geoms AS (
	SELECT 
		t.trip_id,
		ST_Transform(ST_MakeLine(g.geom),32723) AS geom
	FROM rio2014_trips AS t JOIN gps_2014 AS g
		ON g.uid = ANY (t.gps_uids)
	GROUP BY t.trip_id
	ORDER BY t.trip_id
)
UPDATE rio2014_trips SET orig_geom = geoms.geom 
FROM geoms WHERE rio2014_trips.trip_id = geoms.trip_id;

-- delete trips shorter than 100 meters
DELETE FROM rio2014_trips 
WHERE ST_Length(orig_geom) < 100;

-- set the azimuth (for visualization)
UPDATE rio2014_trips SET azimuth = ST_Azimuth(
	ST_StartPoint(orig_geom),
	ST_EndPoint(orig_geom)
);

CLUSTER rio2014_trips USING rio2014_trips_idx;

/*
Create the other necessary tables, some from an imported GTFS dataset
This includes:
- a replacement for 'directions', ordered lists of stops
- stop_times table for storing data
*/

DROP TABLE IF EXISTS rio2014_stop_times;
CREATE TABLE rio2014_stop_times(
	uid serial PRIMARY KEY,
	trip_id integer,
	stop_id varchar,
	stop_sequence integer,
	etime double precision, -- epoch time at greenwich
	arrival_time interval HOUR TO SECOND,
	departure_time interval HOUR TO SECOND
);
CREATE INDEX rio2014_stop_times_idx ON rio2014_stop_times (trip_id);
CLUSTER rio2014_stop_times USING rio2014_stop_times_idx;

-- now the directions table
DROP TABLE IF EXISTS rio2014_directions;
SELECT 
	DISTINCT 
		t.route_id,
		r.route_short_name,
		r.route_long_name,
		r.route_desc,
		t.trip_headsign,
		array_agg(stop_id::text ORDER BY stop_sequence ASC) AS stops
INTO rio2014_directions
FROM 
	gtfs_stop_times AS st 
	JOIN gtfs_trips AS t
		ON st.trip_id = t.trip_id
	JOIN gtfs_routes AS r 
		ON t.route_id = r.route_id
GROUP BY t.route_id, t.trip_headsign, r.route_short_name, r.route_long_name, r.route_desc;
ALTER TABLE rio2014_directions 
	ADD COLUMN direction_id serial PRIMARY KEY;
	
/*
Update trip_ids for vehicles
...this part takes the longest probably
*/

UPDATE rio2014_vehicles SET trip_id = the_trip_id 
FROM (
	SELECT 
		trip_id AS the_trip_id, 
		unnest(vehicle_uids) AS vuid
	FROM rio2014_trips
) AS sub
WHERE vuid = uid;
