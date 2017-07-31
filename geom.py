# contains shapely geometry functions
from shapely.geometry import Point, LineString

def cut(line, distance):
	# Cuts a line in two at a distance from its starting point
	# returns a tuple of lines
	if distance <= 0.0 or distance >= line.length:
		return (
			LineString(),
			LineString(line)
		)
	coords = list(line.coords)
	for i, p in enumerate(coords):
		pd = line.project(Point(p))
		if pd == distance:
			return (
				LineString(coords[:i+1]),	# first section
				LineString(coords[i:]) 		# second section
			)
		if pd > distance:
			cp = line.interpolate(distance)
			return (
				LineString(coords[:i] + [(cp.x, cp.y)]),	# first section 
				LineString([(cp.x, cp.y)] + coords[i:])	# second section
			)
		# if pd < distance: keep going
	# this can fail if the line doubles back on itself.
	print "FAILURE IN LINE CUTTING"
	return (LineString(),LineString())
		
