# call this file to begin processing a set of trips from 
# stored vehicle locations. It will ask which trips from 
# the db to process. For now, I am testing with one at a 
# time or operating on a contiguous range of trips
import multiprocessing as mp
import db
from time import sleep
from block import Block

# let mode be one of ('single','range?')
mode = raw_input('Processing mode (single or range) --> ')

def process_block(valid_block_id):
	"""worker process called when using multiprocessing"""
	print 'starting block:',valid_block_id
	db.reconnect()
	block = Block.fromDB(valid_block_id)
	block.process()

# single mode enters one trip at a time and stops when 
# a non-integer is entered
if mode == 'single':
	block_id = raw_input('block_id to process --> ')
	while block_id.isdigit():
		if db.block_exists(block_id):
			# create a trip object for the block
			this_block = Block.fromDB(block_id)
			# process
			this_block.process()
		else:
			print 'no such block'
		# ask for another block and continue
		block_id = raw_input('block_id to process --> ')

# 'range' mode does all valid ids in the given range
elif mode == 'range':
	id_range = raw_input('block_id range as start:end --> ')
	id_range = id_range.split(':')
	# get a list of block id's in the range
	block_ids = db.get_block_ids(id_range[0],id_range[1])
	print len(block_ids),'blocks in that range'
	# how many parallel processes to use?
	max_procs = int(raw_input('max processes --> '))
	# create a pool of workers and pass them the data
	p = mp.Pool(max_procs)
	p.map(process_block,block_ids)
	print 'COMPLETED!'

else:
	print 'invalid entry mode given' 







