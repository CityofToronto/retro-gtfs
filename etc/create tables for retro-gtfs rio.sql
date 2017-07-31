/*
create the blocks table from python output, and do all that follows
*/

-- read in the results from Python
DROP TABLE IF EXISTS rio2014_blocks;
CREATE TABLE rio2014_blocks (
	-- to be populated from csv vv
	block_id integer, -- this will be set in the next step and made a primary key
	py_id integer, -- block_id given by python
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
COPY rio2014_blocks (block_break_reasons, py_id, vehicle_id, route_id, vehicle_uids) 
FROM '/home/nate/rio/2014blocks.csv' CSV HEADER;

-- reset block_ids, ordered by time, ASC 
WITH new_order AS (
	SELECT 
		b.py_id,
		v.report_time,
		row_number() OVER (ORDER BY v.report_time ASC) AS row_num
	FROM rio2014_blocks AS b JOIN rio2014_vehicles AS v
		ON b.vehicle_uids[1] = v.uid
)
UPDATE rio2014_blocks SET block_id = row_num
FROM new_order WHERE new_order.py_id = rio2014_blocks.py_id;

ALTER TABLE rio2014_blocks ADD PRIMARY KEY (block_id);


-- create an empty trips table, to be filled block by block
-- as they are processed
DROP TABLE IF EXISTS rio2014_trips;
CREATE TABLE rio2014_trips (
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
--UPDATE rio2014_vehicles SET block_id = NULL WHERE block_id IS NOT NULL;
UPDATE rio2014_vehicles SET block_id = the_block_id 
FROM (
	SELECT 
		block_id AS the_block_id, 
		unnest(vehicle_uids) AS vuid
	FROM rio2014_blocks
	--WHERE block_id BETWEEN 270000 AND 280000
) AS sub
WHERE vuid = uid;

/*
Create the other necessary tables, some from an imported GTFS dataset
This includes:
- a replacement for 'directions', ordered lists of stops
- stop_times table for storing data
*/

DROP TABLE IF EXISTS rio2014_stop_times;
CREATE TABLE rio2014_stop_times(
	uid serial PRIMARY KEY,
	trip_id numeric,
	block_id integer, -- not in GTFS but used for quick access by block
	stop_id varchar,
	stop_sequence integer,
	etime double precision, -- interpolated report_time from vehicles table
	arrival_time interval HOUR TO SECOND
);
CREATE INDEX rio2014_stop_times_idx ON rio2014_stop_times (trip_id);
CLUSTER rio2014_stop_times USING rio2014_stop_times_idx;



