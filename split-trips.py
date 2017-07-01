# there is a problem with a stop being twice in a trip
# this script breaks trips in half where any stop 
# appears for a second time. These should still be 
# routable via block_id's with no changes to the graph 

# output file
out = open('../rio/output/stop_times.txt', 'w')
# header
out.write('trip_id,arrival_time,departure_time,stop_id,stop_sequence,block_id\n')

first_line = True
trip = ''
block_id = 0
with open('../rio/output/unsplit_stop_times.txt') as f:
	for line in f:
		if first_line:
			first_line = False
			continue

		# v BUSINESS v
		# read the csv
		trip_id,arrival,departure,stop_id,stop_sequence = line.split(',')
		# see if this is a new trip
		if trip != trip_id:
			trip = trip_id
			trip_suffix = 0
			stops = []
			seq = 0
			block_id += 1
		# see if this stop is already in the trip
		if stop_id not in stops:
			stops.append(stop_id)
			seq += 1
		else: # we have seen this stop before
			stops = []
			trip_suffix += 1
			seq = 1

		out.write(
			trip_id+'.'+str(trip_suffix)+','+
			arrival+','+departure+','+
			stop_id+','+
			str(seq)+','+
			str(block_id)+'\n'
		)


out.close()
