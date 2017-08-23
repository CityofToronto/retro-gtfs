# functions involving BD interaction
import psycopg2, json, math
from conf import conf

# connect and establish a cursor, based on parameters in conf.py
conn_string = (
	"host='"+conf['db']['host']
	+"' dbname='"+conf['db']['name']
	+"' user='"+conf['db']['user']
	+"' password='"+conf['db']['password']+"'"
)
connection = psycopg2.connect(conn_string)
connection.autocommit = True

def reconnect():
	"""renew connections inside a process"""
	global connection
	connection = psycopg2.connect(conn_string)
	connection.autocommit = True

def cursor():
	"""provide a cursor"""
	return connection.cursor()

def new_trip_id():
	"""get a next trip_id to start from, defaulting to 1"""
	c = cursor()
	c.execute("SELECT MAX(trip_id) FROM nb_vehicles;")
	try:
		(trip_id,) = c.fetchone()
		trip_id += 1
	except:
		trip_id = 1
	return trip_id

def new_block_id():
	"""get a next block_id to start from, defaulting to 1"""
	c = cursor()
	c.execute("SELECT MAX(block_id) FROM nb_trips;")
	try:
		(block_id,) = c.fetchone()
		block_id += 1
	except:
		block_id = 1
	return block_id

def empty_tables():
	"""clear the tables"""
	c = cursor()
	c.execute("""
		--TRUNCATE nb_trips;
		--TRUNCATE nb_vehicles;
		TRUNCATE nb_stop_times;
		--TRUNCATE nb_directions;
		--TRUNCATE gtfs_stops;
	""")

def copy_vehicles(filename):
	"""Copy a CSV of vehicle records into the nb_vehicles table.
		This exists because copying is much faster than inserting."""
	c = cursor()
	c.execute("""
		COPY nb_vehicles (trip_id,seq,lon,lat,report_time) FROM %s CSV;
	""",(filename,))


def ignore_block(block_id,reason=None):
	"""mark a trip to be ignored"""
	c = cursor()
	c.execute(
		"""
			UPDATE {blocks} SET ignore = TRUE WHERE block_id = %(block_id)s;
			DELETE FROM {stop_times} WHERE block_id = %(block_id)s;
		""".format(**conf['db']['tables']),
		{'block_id':block_id} )
	if reason:
		flag_block(block_id,reason)
	return


def flag_block(block_id,problem_description_string):
	"""populate 'problem' field of block table: something must 
		have gone wrong"""
	c = cursor()
	c.execute(
		"""
			UPDATE {blocks} 
			SET problem = problem || %(problem)s 
			WHERE block_id = %(block_id)s;
		""".format(**conf['db']['tables']),
		{
			'problem':problem_description_string,
			'block_id':block_id
		}
	)



def add_block_match(block_id,confidence,geometry_match_wkb):
	"""update the block record with it's matched geometry"""
	c = cursor()
	# store the given values
	c.execute(
		"""
			UPDATE {blocks}
			SET  
				match_confidence = %(confidence)s,
				match_geom = ST_SetSRID(%(match_geom)s::geometry,32723)
			WHERE block_id  = %(block_id)s;
		""".format(**conf['db']['tables']),
		{
			'confidence':confidence, 
			'match_geom':geometry_match_wkb, 
			'block_id':block_id
		}
	)


def get_trip_geom(trip_id,geom_type='match'):
	"""returns a trip's geometry ('matched', 'clean', or 'orig') 
		in local coordinates if any exists. This function is 
		really only for testing the rio data"""
	c = cursor()
	if geom_type == 'match':
		c.execute(
			"""
				SELECT match_geom
				FROM {trips}
				WHERE 
					trip_id  = %(trip_id)s
					AND match_geom IS NOT NULL;
			""".format(**conf['db']['tables']),
			{ 'trip_id':trip_id }
		)
		if c.statusmessage == 'SELECT 1':
			(geom,) = c.fetchone()
			return geom
	elif geom_type == 'orig':
		c.execute(
			"""
				SELECT orig_geom
				FROM {trips}
				WHERE 
					trip_id  = %(trip_id)s
					AND orig_geom IS NOT NULL;
			""".format(**conf['db']['tables']),
			{ 'trip_id':trip_id }
		)
		if c.statusmessage == 'SELECT 1':
			(geom,) = c.fetchone()
			return geom
	elif geom_type == 'clean':
		c.execute(
			"""
				SELECT clean_geom
				FROM {trips}
				WHERE 
					trip_id  = %(trip_id)s
					AND clean_geom IS NOT NULL;
			""".format(**conf['db']['tables']),
			{ 'trip_id':trip_id }
		)
		if c.statusmessage == 'SELECT 1':
			(geom,) = c.fetchone()
			return geom


def insert_trip(tid,bid,rid,did,vid):
	"""store the trip in the database"""
	c = cursor()
	# store the given values
	c.execute("""
		INSERT INTO nb_trips 
			( trip_id, block_id, route_id, direction_id, vehicle_id ) 
		VALUES 
			( %s,%s,%s,%s,%s );
	""",
		( tid, bid, rid, did, vid)
	)



def get_stops(direction_id):
	"""given the direction id, get the ordered list of stops
		and their attributes for the direction, returning 
		as a dictionary"""
	# TODO is this actually ordered? Does it need to be?
	# TODO is the geometry still the correct projection?
	c = cursor()
	c.execute(
		"""
			WITH sub AS (
				SELECT
					unnest(stops) AS stop_id
				FROM {directions} 
				WHERE
					direction_id = %(direction_id)s
			)
			SELECT 
				stop_id,
				the_geom [this will break here because you need to check the geom field for projection] 
			FROM {stops} 
			WHERE stop_id IN (SELECT stop_id FROM sub);
		""".format(**conf['db']['tables']),
		{'direction_id':direction_id}
	)
	stops = []
	for (stop_id,geom) in c.fetchall():
		stops.append({
			'id':stop_id,
			'geom':geom
		})
	return stops


def get_nearby_stops(block_id,simplification_distance=1):
	"""return stops within 30m of a block's match geometry
		including ID and local geometry
		Benchmark: this takes ~ 0.10 sec for a big messy linestring
		The buffer function sometimes returns an inexplicable error
		for these big crazy lines, and changing things just a little
		fix it. """
	c = cursor()
	try:
		c.execute(
			"""
				SELECT 
					stop_id,
					ARRAY_LENGTH(former_stop_ids,1) > 1 AS is_cluster,
					centroid
				FROM {stops}
				WHERE 
					ST_Intersects(
						(
							SELECT ST_Simplify(match_geom,%(simp_dist)s) 
							FROM {blocks} 
							WHERE block_id = %(block_id)s 
						),
						the_geom -- buffer geometry
					)
			""".format(**conf['db']['tables']),
			{ 
				'block_id':block_id,
				'simp_dist':simplification_distance
			}
		)
	except:
		# simply simplify the line differently and try again
		return get_nearby_stops(block_id,simplification_distance + 1)

	stops = []
	for (stop_id,is_cluster,geom) in c.fetchall():
		stops.append({'id':stop_id,'is_cluster':is_cluster,'geom':geom})
	return stops


def set_block_orig_geom(block_id,localWKBgeom):
	"""ALL initial vehicles go in this line"""
	c = cursor()
	c.execute(
		"""
			UPDATE {blocks} 
			SET orig_geom = ST_SetSRID( %(geom)s::geometry, %(EPSG)s )
			WHERE block_id = %(block_id)s;
		""".format(**conf['db']['tables']),
		{
			'block_id':block_id,
			'geom':localWKBgeom,
			'EPSG':conf['localEPSG']
		}
	)


def set_block_clean_geom(block_id,localWKBgeom):
	"""Store a geometry of the input to the matching process"""
	c = cursor()
	c.execute(
		"""
			UPDATE {blocks} 
			SET clean_geom = ST_SetSRID( %(geom)s::geometry, %(EPSG)s )
			WHERE block_id = %(block_id)s;
		""".format(**conf['db']['tables']),
		{
			'block_id':block_id,
			'geom':localWKBgeom,
			'EPSG':conf['localEPSG']
		}
	)



def finish_block(block):
	"""1. store stop info in stop_times
		2. determine the service_id and set it per trip.
		3. set the arrival and departure times based on the day start"""
	c = cursor()
	# insert the stops
	records = []
	seq = 1
	for stop in block.stops:
		# list of tuples
		records.append( (block.block_id,stop['id'],stop['arrival'],seq) )
		seq += 1
	args_str = ','.join(c.mogrify("(%s,%s,%s,%s)", x) for x in records)
	c.execute(
		"""
			INSERT INTO {stop_times} 
				(block_id, stop_id, etime, stop_sequence) 
			VALUES 
		""".format(**conf['db']['tables']) + args_str
	)

	# get the first start time
	t = block.stops[0]['arrival']
	# find the etime of the first moment of the day
	# first center the day on local time
	tlocal = t - 4*3600
	from_dawn = tlocal % (24*3600)
	# service_id is distinct to local day
	service_id = (tlocal-from_dawn)/(24*3600)
	day_start = t - from_dawn
	c.execute(
		"""
			UPDATE {blocks} 
			SET service_id = %(service_id)s 
			WHERE block_id = %(block_id)s;
		""".format(**conf['db']['tables']),
		{
			'service_id':service_id,
			'block_id':block.block_id
		}
	)

	# set the arrival and departure times
	c.execute(
		"""
			UPDATE {stop_times} SET 
				arrival_time = ROUND(etime - %(day_start)s) * INTERVAL '1 second'
			WHERE block_id = %(block_id)s;
		""".format(**conf['db']['tables']),
		{
			'day_start':day_start,
			'block_id':block.block_id
		}
	)

def store_trips(block_id,route_id,trips):
	"""store all trip-level and stop level information from the 
		processing of a block"""	
	c = cursor()
	t = 0.001
	for trip in trips:
		# make up a unique trip_id by adding a decimal to 
		# the unique block_id
		trip_id = int(block_id) + t
		t += 0.001
		# the service_id is based on the day of the first stop_time
		t1 = trip['stops'][0]['arrival']
		service_id = math.floor( t1 / (24*60*60) )
		# first handle the trip
		c.execute("""
				INSERT INTO {trips} ( 
					trip_id, block_id, service_id, route_id, 
					shape_geom 
				)
				VALUES ( 
					%(trip_id)s, %(block_id)s, %(service_id)s, %(route_id)s, 
					ST_SetSRID(%(geom)s::geometry,%(localEPSG)s)
				);
			""".format(**conf['db']['tables']),
			{
				'trip_id':trip_id,
				'block_id':block_id,
				'service_id':service_id,
				'route_id':route_id,
				'geom':trip['geom'],
				'localEPSG':conf['localEPSG']
			}
		)
		# then handle the stops
		seq = 1
		for stop in trip['stops']:	
			c.execute("""
					INSERT INTO {stop_times} 
						( trip_id, block_id, stop_id, etime, stop_sequence )
					VALUES 
						( %(trip_id)s, %(block_id)s, %(stop_id)s, %(etime)s, %(seq)s );
				""".format(**conf['db']['tables']),
				{
					'trip_id':trip_id,
					'block_id':block_id,
					'stop_id':stop['id'],
					'etime':stop['arrival'],
					'seq':seq
				}
			)
			seq += 1



def try_storing_stop(stop_id,stop_name,stop_code,lon,lat):
	"""we have received a report of a stop from the routeConfig
		data. Is this a new stop? Have we already heard of it?
		Decide whether to store it or ignore it. If absolutely
		nothing has changed about the record, ignore it. If not,
		store it with the current time."""
	c = cursor()
	# see if precisely this record already exists
	c.execute("""
		SELECT * FROM gtfs_stops
		WHERE 
			stop_id = %s AND
			stop_name = %s AND
			stop_code = %s AND
			ABS(lon - %s::numeric) <= 0.0001 AND
			ABS(lat - %s::numeric) <= 0.0001;
	""",( stop_id,stop_name,stop_code,lon,lat ) )
	# if any result, we already have this stop
	if c.rowcount > 0:
		return
	# store the stop
	c.execute("""
		INSERT INTO gtfs_stops ( 
			stop_id, stop_name, stop_code, 
			the_geom, 
			lon, lat, report_time 
		) 
		VALUES ( 
			%s, %s, %s, 
			ST_Transform( ST_SetSRID( ST_MakePoint(%s, %s),4326),26917 ),
			%s, %s, NOW()
		)""",( 
			stop_id,stop_name,stop_code,
			lon,lat,
			lon,lat #,time
		) )


def try_storing_direction(route_id,did,title,name,branch,useforui,stops):
	"""we have recieved a report of a route direction from the 
		routeConfig data. Is this a new direction? Have we already 
		heard of it? Decide whether to store it or ignore it. If 
		absolutely nothing has changed about the record, ignore it. 
		If not, store it with the current time."""
	c = cursor()
	# see if exactly this record already exists
	c.execute("""
		SELECT * FROM nb_directions
		WHERE
			route_id = %s AND
			direction_id = %s AND
			title = %s AND
			name = %s AND
			branch = %s AND
			useforui = %s AND
			stops = %s;
	""",(route_id,did,title,name,branch,useforui,stops))
	if c.rowcount > 0:
		return # already have the record
	# store the data
	c.execute("""
		INSERT INTO nb_directions 
			( 
				route_id, direction_id, title, 
				name, branch, useforui, 
				stops, report_time
			) 
		VALUES 
			( 
				%s, %s, %s,
				%s, %s, %s, 
				%s, NOW()
			)""",(
				route_id,did,title,
				name,branch,useforui,
				stops
			)
		)


def scrub_block(block_id):
	"""Un-mark any flag fields and leave the DB record 
		as though newly collected and unprocessed"""
	c = cursor()
	c.execute(
		"""
			-- Blocks table
			UPDATE {blocks} SET 
				match_confidence = NULL,
				match_geom = NULL,
				clean_geom = NULL,
				problem = '',
				ignore = FALSE 
			WHERE block_id = %(block_id)s;

			-- Trips table
			DELETE FROM {trips} WHERE block_id = %(block_id)s;

			-- Stop-Times table
			DELETE FROM {stop_times} WHERE block_id = %(block_id)s;
		""".format(**conf['db']['tables']),
		{'block_id':block_id}
	)



def get_trip(trip_id):
	"""return the attributes of a stored trip necessary 
		for the construction of a new trip object"""
	c = cursor()
	c.execute(
		"""
			SELECT 
				block_id, direction_id, route_id, vehicle_id 
			FROM {trips}
			WHERE trip_id = %(trip_id)s
		""".format(**conf['db']['tables']), 
		{'trip_id':trip_id}
	)
	(bid,did,rid,vid,) = c.fetchone()
	return (bid,did,rid,vid)


def get_block(block_id):
	"""return the attributes of a stored block necessary 
		for the construction of a new trip object"""
	c = cursor()
	c.execute(
		"""
			SELECT 
				route_id, vehicle_id 
			FROM {blocks}
			WHERE block_id = %(block_id)s
		""".format(**conf['db']['tables']), 
		{'block_id':block_id}
	)
	(rid,vid,) = c.fetchone()
	return (rid,vid)


def get_trip_ids(min_id,max_id):
	"""return a list of all trip ids in the specified range"""
	c = cursor()
	c.execute(
		"""
			SELECT trip_id 
			FROM {trips}
			WHERE trip_id BETWEEN %(min)s AND %(max)s 
			ORDER BY trip_id ASC
		""".format(**conf['db']['tables']),
		{'min':min_id,'max':max_id}
	)
	return [ result for (result,) in c.fetchall() ]

def get_block_ids(min_id,max_id):
	"""return a list of all block ids in the specified range"""
	c = cursor()
	c.execute(
		"""
			SELECT block_id 
			FROM {blocks}
			WHERE block_id BETWEEN %(min)s AND %(max)s 
			ORDER BY block_id ASC
		""".format(**conf['db']['tables']),
		{'min':min_id,'max':max_id}
	)
	return [ result for (result,) in c.fetchall() ]


def trip_exists(trip_id):
	"""check whether a trip exists in the database, 
		returning boolean"""
	c = cursor()
	c.execute(
		"""
			SELECT EXISTS (
				SELECT * FROM {trips} 
				WHERE trip_id = %(trip_id)s)
		""".format(**conf['db']['tables']),
		{'trip_id':trip_id}
	)
	(existence,) = c.fetchone()
	return existence


def block_exists(block_id):
	"""check whether a block exists in the database, 
		returning boolean"""
	c = cursor()
	c.execute(
		"""
			SELECT EXISTS (
				SELECT * FROM {blocks} 
				WHERE block_id = %(block_id)s)
		""".format(**conf['db']['tables']),
		{'block_id':block_id}
	)
	(existence,) = c.fetchone()
	return existence


def get_vehicles(block_id):
	"""returns full projected vehicle linestring and times"""
	c = cursor()
	# get the trip geometry and timestamps
	c.execute(
		"""
			SELECT
				uid, ST_Y(geom) AS lat, ST_X(geom) AS lon, report_time,
				ST_Transform(geom,%(EPSG)s) AS geom
			FROM {vehicles} 
			WHERE block_id = %(block_id)s
			ORDER BY report_time ASC;
		""".format(**conf['db']['tables']),
		{
			'block_id':block_id,
			'EPSG':conf['localEPSG']
		}
	)
	vehicles = []
	for (uid,lat,lon,time,geom) in c.fetchall():
		vehicles.append({
			'uid':	uid,
			'geom':	geom,
			'time':	time,
			'lat':	lat,
			'lon':	lon
		})
	return vehicles

def get_route_directions(route_id):
	"""This may only be useful for the rio data.
		Return direction_id's for a given route"""
	c = cursor()
	# remove any decimal from the route_id
	route_id = route_id.split('.')[0]
	c.execute(
		"""
			SELECT 
				direction_id
			FROM {directions} 
			WHERE route_short_name = %(route_id)s
		""".format(**conf['db']['tables']),
		{'route_id':route_id}
	)
	# return a list of direction IDs


	return [ did for (did,) in c.fetchall() ]


