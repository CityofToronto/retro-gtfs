/*
create the blocks table from python output, and do all that follows
*/

-- read in the results from Python
DROP TABLE IF EXISTS rio2017_blocks;
CREATE TABLE rio2017_blocks (
	-- to be populated from csv vv
	block_id integer PRIMARY KEY,
	vehicle_id varchar,
	route_id varchar,
	vehicle_uids integer[],
	block_break_reasons text[],
	-- to be populated from csv ^^
	-- comes later vv
	orig_geom geometry(LINESTRING,32723),	-- geometry of all vehicle points
	clean_geom geometry(LINESTRING,32723), -- geometry of points used in map matching
	match_geom geometry(LINESTRING,32723), -- map-matched route geometry
	match_confidence real, -- 0 - 1
	-- for manual cleaning vv
	ignore boolean DEFAULT FALSE, -- ignore this block during processing/extraction?
	km real, -- length of geometry
	problem text -- records any problems that cause this ot to be used
);
COPY rio2017_blocks (block_break_reasons, block_id, vehicle_id, route_id, vehicle_uids) 
FROM '/home/nate/rio/2017blocks.csv' CSV HEADER;

CREATE INDEX ON rio2017_blocks ();

-- populate geometry column with one big join/update
/*
-- this isn't necessary and takes too long
WITH geoms AS (
	SELECT 
		b.block_id,
		ST_Transform(ST_MakeLine(v.geom ORDER BY report_time ASC),32723) AS geom
	FROM rio2017_blocks AS b JOIN rio2017_vehicles AS v
		ON v.uid = ANY (b.vehicle_uids)
	WHERE b.orig_geom IS NULL AND b.block_id BETWEEN 205000 AND 210000
	GROUP BY b.block_id
)
UPDATE rio2017_blocks SET orig_geom = geoms.geom 
FROM geoms WHERE rio2017_blocks.block_id = geoms.block_id;

UPDATE rio2017_blocks SET km = ST_Length(orig_geom)/1000
WHERE km IS NULL AND orig_geom IS NOT NULL;
*/

-- create an empty trips table, to be filled block by block
-- as they are processed
DROP TABLE IF EXISTS rio2017_trips;
CREATE TABLE rio2017_trips (
	trip_id numeric PRIMARY KEY,
	block_id integer,
	service_id integer,
	route_id text,
	shape_geom geometry(LINESTRING,32723) -- portion of match geometry if applicable
);

/*
Update block_ids for vehicles
...this part takes the longest probably
*/
UPDATE rio2017_vehicles SET block_id = NULL WHERE block_id IS NOT NULL;
UPDATE rio2017_vehicles SET block_id = the_block_id 
FROM (
	SELECT 
		block_id AS the_block_id, 
		unnest(vehicle_uids) AS vuid
	FROM rio2017_blocks
	WHERE block_id <= 10000
) AS sub
WHERE vuid = uid;

/*
Create the other necessary tables, some from an imported GTFS dataset
This includes:
- a replacement for 'directions', ordered lists of stops
- stop_times table for storing data
*/

DROP TABLE IF EXISTS rio2017_stop_times;
CREATE TABLE rio2017_stop_times(
	uid serial PRIMARY KEY,
	trip_id numeric,
	block_id integer, -- not in GTFS but used for quick access by block
	stop_id varchar,
	stop_sequence integer,
	etime double precision, -- interpolated report_time from vehicles table
	arrival_time interval HOUR TO SECOND
);
CREATE INDEX rio2017_stop_times_idx ON rio2017_stop_times (trip_id);
CLUSTER rio2017_stop_times USING rio2017_stop_times_idx;


-- set arrival times in stop_times table 




