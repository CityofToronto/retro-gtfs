# documentation on the nextbus feed:
# http://www.nextbus.com/xmlFeedDocs/NextBusXMLFeed.pdf

import re, db, json, map_api, random, math
from numpy import mean
from conf import conf
from shapely.wkb import loads as loadWKB, dumps as dumpWKB
from shapely.ops import transform as reproject
from shapely.geometry import asShape, Point, LineString
from geom import cut


class Trip(object):
	"""The trip class provides all the methods needed for dealing
		with one observed trip/track. Classmethods provide two 
		different ways of instantiating."""

	def __init__(self,trip_id,block_id,direction_id,route_id,vehicle_id,last_seen):
		"""initialization method, only accessed by the @classmethod's below"""
		# set initial attributes
		self.trip_id = trip_id				# int
		self.block_id = block_id			# int
		self.direction_id = direction_id	# str
		self.route_id = route_id			# int
		self.vehicle_id = vehicle_id		# int
		self.last_seen = last_seen			# last vehicle report (epoch time)
		# initialize sequence
		self.seq = 1					# sequence which increments at each report
		# declare several vars for later in the matching process
		self.speed_string = ""		# str
		self.match_confidence = -1	# 0 - 1 real
		self.nearby_stops = []		# all stops close to the match geometry
		self.stops = []				# all stops with arrivals
		self.segment_speeds = []	# reported speeds of all segments
		self.length = 0				# length in meters of current string
		self.vehicles = []			# ordered vehicle records
		self.problems = []			# running list of issues
		self.match_geom = None		# map-matched linestring 

	@classmethod
	def new(clss,trip_id,block_id,direction_id,route_id,vehicle_id,last_seen):
		"""create wholly new trip object, providing all paremeters"""
		# store instance in the DB
		db.insert_trip( trip_id, block_id, route_id, direction_id, vehicle_id )
		return clss(trip_id,block_id,direction_id,route_id,vehicle_id,last_seen)

	@classmethod
	def fromDB(clss,trip_id):
		"""construct a trip object from an existing record in the database"""
		(bid,did,rid,vid) = db.get_trip(trip_id)
		return clss(trip_id,bid,did,rid,vid,last_seen=None)

	def process(self):
		"""A trip has just ended. What do we do with it?"""
		db.scrub_trip(self.trip_id)
		# get vehicle records and make geometry objects
		self.vehicles = db.get_vehicles(self.trip_id)
		for v in self.vehicles:
			v['geom'] = loadWKB(v['geom'],hex=True)
			v['ignore'] = False
		# update the pre-cleaning geometry
		# TODO remove for speed
		db.set_trip_orig_geom(self.trip_id,self.get_geom())
		# calculate vector of segment speeds
		self.segment_speeds = self.get_segment_speeds()
		# check for very short trips
		if self.length < 0.8: # km
			return db.ignore_trip(self.trip_id,'too short')
		# check for errors and attempt to correct them
		while self.has_errors():
			# make sure it's still long enough to bother with
			if len(self.vehicles) < 3:
				return db.ignore_trip(self.trip_id,'error processing made too short')
			# still long enough to try fixing
			self.fix_error()
			# update the segment speeds for the next iteration
			self.segment_speeds = self.get_segment_speeds()
		# trip is clean, so store the cleaned line and begin matching
		db.set_trip_clean_geom(self.trip_id,self.get_geom())
		self.match()

	def get_geom(self):
		"""return a clean WKB geometry string using all vehicles
			in the local projection"""
		line = []
		for v in self.vehicles:
			line.append(v['geom'])
		return dumpWKB(LineString(line),hex=True)

	def get_segment_speeds(self):
		"""return speeds (kmph) on the segments between vehicle records"""
		# iterate over segments (i-1)
		dists = []	# km
		times = []	# hours
		for i in range(1,len(self.vehicles)):
			v1 = self.vehicles[i-1]
			v2 = self.vehicles[i]
			# distance in kilometers
			dists.append( v1['geom'].distance(v2['geom'])/1000 )
			# time in hours
			times.append( (v2['time']-v1['time'])/3600 )
		# set the total distance
		self.length = sum(dists)
		# calculate speeds
		return [ d/t for d,t in zip(dists,times) ]


	def match(self):
		"""Match the trip to the road network, and do all the
			things that follow therefrom."""
		# don't use times for now TODO use them
		result = map_api.map_match(self.vehicles,False)
		# flag results with multiple matches for now until you can 
		# figure out exactly what is going wrong
		if result['code'] != 'Ok':
			return self.flag('match problem, code not "Ok"')
		if len(result['matchings']) > 1:
			return self.flag('more than one match segment')
		# only handling the first result for now TODO fix that
		match = result['matchings'][0]
		self.match_confidence = match['confidence']
		# report on match quality
		print '\t',self.match_confidence
		# store the trip geometry
		self.match_geom = asShape(match['geometry'])
		# and be sure to project it correctly...
		self.match_geom = reproject( conf['projection'], self.match_geom )
		# simplify slightly for speed
		self.match_geom = self.match_geom.simplify(1)
		# add geometries for debugging. Remove for faster action
		db.add_trip_match(
			self.trip_id,
			self.match_confidence,
			dumpWKB(self.match_geom,hex=True)
		)
		# get the stops as a list of objects
		# with keys {'id':stop_id,'geom':geom}
		self.nearby_stops = db.get_nearby_stops(self.trip_id)
		# parse the geometries
		for stop in self.nearby_stops:
			stop['geom'] = loadWKB(stop['geom'],hex=True)
		# use the OSRM tracepoints to drop vehicles that did 
		# not contribute to the match 
		tracepoints = result['tracepoints']
		for i in reversed( range( 0, len(self.vehicles) ) ):
			# these are the matched points of the input cordinates
			# null entries indicate an omitted outlier
			if tracepoints[i] is None:
				del self.vehicles[i]
		# we should now have one more vehicle record than we have 
		# legs to the match result. Use these to assign intervehicle 
		# distances to the vehicle records
		for i in range(0,len(self.vehicles)):
			if i == 0:
				self.vehicles[i]['dist_from_last'] = 0
				self.vehicles[i]['cum_dist'] = 0
			else:
				self.vehicles[i]['dist_from_last'] = match['legs'][i-1]['distance']
				self.vehicles[i]['cum_dist'] = self.vehicles[i-1]['cum_dist'] + match['legs'][i-1]['distance']
		# now match stops to the trip geometry
		# iterate over 750m sections of the match geometry
		path = self.match_geom
		traversed = 0
		# while there is more than 750m of path remaining
		# this loop takes more than a second!
		from time import time
		start = time()
		while path.length > 750:
			subpath, path = cut(path,750)
			# check for nearby stops
			for stop in self.nearby_stops:
				stop_dist = subpath.distance(stop['geom'])
				if stop_dist <= 30:
					# the stop is close to the line!
					# find the measure along the total path
					stop_m = traversed + subpath.project(stop['geom'])
					self.add_stop(stop,stop_m,stop_dist)
			# note that we have traversed an additional 500m
			traversed += 750
		print 'locating stops took ',time() - start,'for',self.trip_id
		# interpolate stop times
		for stop in self.stops:
			# interpolate a time
			stop['arrival'] = self.interpolate_time(stop)
		# ensure that the stops are ordered
		self.stops = sorted(self.stops, key=lambda k: k['arrival']) 
		# there is more than one stop, right?
		if len(self.stops) > 1:
			db.finish_trip(self)
		else:
			db.ignore_trip(self.trip_id,'fewer than two stop times estimated')
		return

	def add_stop(self,new_stop,new_measure,new_distance):
		"""add a stop observation or update an existing one"""
		# we are looking to avoid adding the same stop twice
		for stop in self.stops:
			# same stop id and close to the same position?
			if stop['id']==new_stop['id'] and abs(stop['measure']-new_measure) < 50:
				# keep the one that is closer
				if stop['dist'] <= new_distance:
					# the stop we already have is closer
					return
				else:	
					# the new stop is closer
					stop['measure'] = new_measure
					stop['dist'] = new_distance
					return
		# we don't have anything like this stop yet, so add it
		self.stops.append({
			'id':new_stop['id'],
			'measure':new_measure,
			'dist':new_distance
		})

	def flag(self,problem_description):
		"""record that something undesireable has occured"""
		self.problems.append(problem_description)


	def has_errors(self):
		"""see if the speed segments indicate that there are any 
			fixable errors by making the speed string and checking
			for fixeable patterns."""
		# convert the speeds into a string
		self.speed_string = ''.join([ 
			'x' if segSpeed > 80.0 else 'o' if segSpeed < 3.0 else '-'
			for segSpeed in self.segment_speeds ])
		# do RegEx search for 'xx' or 'oo'
		match_oo = re.search('oo',self.speed_string)
		match_x = re.search('xx',self.speed_string)
		if match_oo or match_x:
			return True
		else:
			return False


	def fix_error(self):
		"""remove redundant points and fix obvious positional 
			errors using RegEx. Fixes one error each time it's 
			called: the first it finds"""
		# check for leading o's (stationary start)
		m = re.search('^oo*',self.speed_string)
		if m: # remove the first vehicle
			self.vehicles.pop( 0 )
			return
		# check for trailing o's (stationary end)
		m = re.search('oo*$',self.speed_string)
		if m: # remove the last vehicle
			self.vehicles.pop( len(self.speed_string) )
			return
		# check for x near beginning, in first four segs
		m = re.search('^.{0,3}x',self.speed_string)
		if m: # remove the first vehicle
			self.vehicles.pop( 0 )
			return
		# check for x near the end, in last four segs
		m = re.search('x.{0,3}$',self.speed_string)
		if m: # remove the last vehicle
			self.vehicles.pop( len(self.speed_string) )
			return
		# check for two or more o's in the middle and take from after the first o
		match = re.search('ooo*',self.speed_string)
		if match:
			# remove all vehicles between the first and last o's
			v1index = match.span()[0]+1
			v2index = match.span()[1]
			# this has to be reversed because the indices reorder 
			for Vindex in reversed(range(v1index,v2index)):
				self.vehicles.pop(Vindex)
			return
		# 'xx' in the middle, delete the point after the first x
		m = re.search('.xxx*',self.speed_string)
		if m:
			# same strategy as above
			self.vehicles.pop( m.span()[0]+2 )
			return
#		# lone middle x
#		m = re.search('.x.',self.speed_string)
#		if m:
#			# delete a point either before or after a lone x
#			i = m.span()[0]+1+random.randint(0,1)
#			self.vehicles.pop( i-1 )
#			return


	def interpolate_time(self,stop):
		"""get the time for a stop by doing an interpolation between 
			vehicle locations. We already know the m of the stop
			and of the vehicles on the trip/track"""
		# iterate over the segments of the trip, looking for the segment
		# which holds the stop of interest
		first = True
		for v in self.vehicles:
			if first:
				m1 = v['cum_dist'] # zero
				t1 = v['time'] # time
				first = False
				continue
			m2 = v['cum_dist']
			t2 = v['time']
			# if intersection is at or between these points
			if m1 <= stop['measure'] <= m2:
				# interpolate the time
				if stop['measure'] == m1:
					return t1
				percent_of_segment = (stop['measure'] - m1) / (m2 - m1)
				additional_time = percent_of_segment * (t2 - t1) 
				return t1 + additional_time
			# create the segment for the next iteration
			m1,t1 = m2,t2

		# if we've made it this far, the stop was not technically on or 
		# between any waypoints. This is probably a precision issue and the 
		# stop should be right off one of the ends. Add 20 seconds as a 
		# guestimate for extra time
		if stop['measure'] == 0:
			return self.vehicles[0]['time'] - 20
		elif stop['measure'] == 1:
			return self.vehicles[-1]['time'] + 20
		else:
			print '\t\tstop thing failed??'
		return None


