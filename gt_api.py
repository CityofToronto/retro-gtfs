import protobuf
import requests
import gtfs_realtime_pb2

def get_vehicles():
	# grab the files and parse them:
	v_url = 'http://developer.go-metro.com/TMGTFSRealTimeWebService/vehicle'
	data = requests.get(v_url).content
	vehicles = gtfs_realtime_pb2.FeedMessage()
	vehicles.ParseFromString(data)
	return vehicles

def get_updates():
	u_url = 'http://developer.go-metro.com/TMGTFSRealTimeWebService/TripUpdate'
	data = requests.get(u_url).content
	updates = gtfs_realtime_pb2.FeedMessage()
	updates.ParseFromString(data)
	return updates
