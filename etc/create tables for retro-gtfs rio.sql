/*
create the trips table from python output and  
generate geometries with azimuth for visualization
*/

-- read in the results from Python
DROP TABLE IF EXISTS rio2017_trips;
CREATE TABLE rio2017_trips (
	-- to be populated from csv vv
	trip_id integer PRIMARY KEY,
	block_id integer,
	vehicle_id varchar,
	route_id varchar,
	vehicle_uids integer[],
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
	azimuth real, 			-- azimuth for pretty rendering
	trip_break_reasons text[],
	block_break_reasons text[]

);
COPY rio2017_trips (trip_break_reasons, block_break_reasons, trip_id, block_id, vehicle_id, route_id, vehicle_uids) 
FROM '/home/nate/rio/2017trips.csv' CSV HEADER;

-- populate geometry column with one big join/update
WITH geoms AS (
	SELECT 
		t.trip_id,
		ST_Transform(ST_MakeLine(v.geom),32723) AS geom
	FROM rio2017_trips AS t JOIN rio2017_vehicles AS v
		ON v.uid = ANY (t.vehicle_uids)
	GROUP BY t.trip_id
	ORDER BY t.trip_id
)
UPDATE rio2017_trips SET orig_geom = geoms.geom 
FROM geoms WHERE rio2017_trips.trip_id = geoms.trip_id;

-- delete trips shorter than 100 meters
DELETE FROM rio2017_trips 
WHERE ST_Length(orig_geom) < 100;

-- set the azimuth (for visualization)
UPDATE rio2017_trips SET azimuth = ST_Azimuth(
	ST_StartPoint(orig_geom),
	ST_EndPoint(orig_geom)
);

CLUSTER rio2017_trips USING rio2017_trips_idx;

/*
Create the other necessary tables, some from an imported GTFS dataset
This includes:
- a replacement for 'directions', ordered lists of stops
- stop_times table for storing data
*/

DROP TABLE IF EXISTS rio2017_stop_times;
CREATE TABLE rio2017_stop_times(
	uid serial PRIMARY KEY,
	trip_id integer,
	stop_id varchar,
	stop_sequence integer,
	etime double precision, -- epoch time at greenwich
	arrival_time interval HOUR TO SECOND,
	departure_time interval HOUR TO SECOND
);
CREATE INDEX rio2017_stop_times_idx ON rio2017_stop_times (trip_id);
CLUSTER rio2017_stop_times USING rio2017_stop_times_idx;


-- now the directions table
DROP TABLE IF EXISTS rio2017_directions;
WITH stop_sequences AS (
	SELECT 
		trip_id,
		array_agg(stop_id::text ORDER BY stop_sequence ASC) AS stops
	FROM gtfs_2017_stop_times
	GROUP BY trip_id
)
SELECT 
	DISTINCT 
		t.route_id,
		r.route_short_name,
		r.route_long_name,
		r.route_desc,
		r.route_url,
		t.trip_headsign,
		t.shape_id,
		ss.stops
INTO rio2017_directions
FROM stop_sequences AS ss 
	JOIN gtfs_2017_trips AS t
		ON ss.trip_id = t.trip_id
	JOIN gtfs_2017_routes AS r 
		ON t.route_id = r.route_id
GROUP BY t.route_id, ss.stops, t.trip_headsign, t.shape_id, r.route_short_name, r.route_url, r.route_long_name, r.route_desc;
ALTER TABLE rio2017_directions 
	-- primary key
	ADD COLUMN direction_id serial PRIMARY KEY,
	-- geometry from shapes table
	ADD COLUMN shape geometry(LINESTRING,4326);
-- add shape geometries
WITH shapes AS (
	SELECT 
		shape_id,
		ST_MakeLine(
			ST_SetSRID(ST_MakePoint(shape_pt_lon,shape_pt_lat),4326)
		ORDER BY shape_pt_sequence ASC ) AS line
	FROM gtfs_2017_shapes 
	GROUP BY shape_id
) UPDATE rio2017_directions SET shape = line
FROM shapes WHERE shapes.shape_id = rio2017_directions.shape_id
	
/*
Update trip_ids for vehicles
...this part takes the longest probably
*/

UPDATE rio2017_vehicles SET trip_id = the_trip_id 
FROM (
	SELECT 
		trip_id AS the_trip_id, 
		unnest(vehicle_uids) AS vuid
	FROM rio2017_trips
) AS sub
WHERE vuid = uid;
