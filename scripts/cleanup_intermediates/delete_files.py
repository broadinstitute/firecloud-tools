from common import *

import json
from apiclient.discovery import build
from apiclient.http import BatchHttpRequest
from itertools import islice
import requests.auth as mymodule
import sys

def batch_callback(request_id, response, exception): 
	# if exception:
# 		print exception
	pass
	
def batch_delete(object_to_bucket_dict):
	batch_delete_requests = service.new_batch_http_request(callback=batch_callback)
	for object, bucket_name in object_to_bucket_dict.iteritems():
		batch_delete_requests.add(collection.delete(bucket=bucket_name, object=object))
		mymodule.pb.increment()
	batch_delete_requests.execute()
	mymodule.pb.print_bar()

def initProcess(pb):
 	mymodule.pb = pb
 		
def chunk_dict(data, SIZE=10000):
	it = iter(data)
	for i in xrange(0, len(data), SIZE):
		yield {k:data[k] for k in islice(it, SIZE)}

if __name__ == "__main__":
	if len(sys.argv) < 2:
		fail("Please provide an input file of file paths as the argument to this script.")
		
	service = build('storage', 'v1')
	collection = service.objects()

	with open(sys.argv[1], 'rU') as in_file:
		list_blobs = in_file.read().splitlines()

	# remove the gs://bucket_name from the path 
	list_objects = {}
	for file_path in list_blobs:
		bucket_name = re.search('gs://([^/]*)/', file_path).group(1)
		list_objects[re.sub(r'gs://[^/]*/','', file_path)] = bucket_name
	
	n=1000
	num_objects_to_delete = len(list_objects)
	chunks=list(chunk_dict(list_objects, n))
	num_chunks = len(chunks)

	original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
	pb = ProgressBar(0, num_objects_to_delete, "Files deleted")
	pool = mp.Pool(initializer=initProcess,initargs=(pb,), processes=5)
	signal.signal(signal.SIGINT, original_sigint_handler)
	
	# TODO: Needs better error handling for 502 Gateway Errors
	#[result.get() for result in [pool.apply_async(batch_delete, args=(object_to_bucket_dict,)) for object_to_bucket_dict in chunks]]
	[pool.apply(batch_delete, args=(object_to_bucket_dict,)) for object_to_bucket_dict in chunks]

		