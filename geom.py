# contains shapely geometry functions
from shapely.geometry import Point, LineString

def subset_line(line,m1=0,m2=9**99):
	"""subset a given linestring between two distances along 
		its length. Returns a linestring."""
	# error checking
	if not m1 >= 0 and m1 <= line.length:
		raise SystemExit('invalid input for m1')
	if not m2 > 0 and m2 <= line.length:
		raise SystemExit('invalid input for m2')
	if not m2 > m1:
		raise SystemExit('invalid input for m1,2')
	# first cut at m1
	subline = []
	passed = False
	for index, point in enumerate(list(line.coords)):
		if not passed:
			pm = line.project(Point(point))
			if pm < m1:
				continue
			elif pm >= m1:
				# we have passed m1 and will add it to the new line
				subline.append(line.interpolate(m1))
				passed = True
		elif passed: 
			subline.append(point)
	# set the line to the new geometry cut at m1
	line = LineString(subline) 
	# now repeat for m2
	subline = []
	for index, point in enumerate(list(line.coords)):
		pm = line.project(Point(point))
		# have we reached/passed m2 yet?
		if pm >= m2:
			subline.append(line.interpolate(m2))
			return LineString(subline)
		else: 
			subline.append(point)
	raise SystemExit('we should not be here')
