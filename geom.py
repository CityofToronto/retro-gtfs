# contains shapely geometry functions
from shapely.geometry import Point, LineString

def subset_line(line,m1=0,m2=9**99):
	"""subset a given linestring between two distances along 
		its length. Returns a linestring.
		I'm pretty sure this is working, but I really ought to 
		give it a good visual test!"""
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
	for index, xy in enumerate(list(line.coords)):
		if not passed:
			pm = line.project(Point(xy))
			if pm < m1:
				continue
			elif pm >= m1:
				# we have passed m1 and will add it to the new line
				p = line.interpolate(m1)
				subline.append((p.x,p.y))
				passed = True
		elif passed: 
			subline.append(xy)
	# set the line to the new geometry cut at m1
	line = LineString(subline) 
	# now repeat for m2
	m2 -= m1 # because this is opertaing on the already subsetted line
	subline = []
	for index, xy in enumerate(list(line.coords)):
		pm = line.project(Point(xy))
		# have we reached/passed m2 yet?
		if pm >= m2:
			p = line.interpolate(m2)
			subline.append((p.x,p.y))
			line = LineString(subline)
			return line
		else: 
			subline.append(xy)
	raise SystemExit('we should not be here')
