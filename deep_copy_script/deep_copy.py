#!/usr/local/bin/python


from six import  iteritems
from argparse import ArgumentParser
import json
import firecloud.api as fapi
import firecloud.fiss as fcfiss
import subprocess

def recursiveBucketCopy(p,w,dp,dw):
	sBucket=getGSBucket(p,w)
	dBucket=getGSBucket(dp,dw)
	#print "sBucket is ",sBucket
	#print "dBucket is ",dBucket
	bucket_cp_cmd="gsutil -m cp -r "+sBucket+"/* "+dBucket
	print "The gsutil copy command to be run is "+bucket_cp_cmd
	output=subprocess.check_output(bucket_cp_cmd, shell=True)
	if(len(output)>0):
		print "\nOutput from gsutil : \n"+output
	

def getAccessToken():
	command="gcloud auth print-access-token"
	output=subprocess.check_output(command, shell=True)
	token_cache=output.strip()
	return token_cache




def doesUserSeeWorkspace(p,w):
	#print "find out"
	api_url="https://api.firecloud.org/api"
	r = fapi.list_workspaces(api_url)
	fapi._check_response_code(r, 200)
	#print "r is ",r
	#Parse the JSON for the workspace + namespace
	workspaces = r.json()

	#pretty_spaces = []
	for space in workspaces:
		#print "spsace is ",space
		ns = str(space['workspace']['namespace'])
		ws = str(space['workspace']['name'])
		if(ns==p and ws==w):
			return True
	return False


def cloneWS(p,w,dp,dw):
	r = fapi.clone_workspace(p,w,dp,dw, "https://api.firecloud.org/api")
	fapi._check_response_code(r, 201) 
	#msg =  args.project + '/' + args.workspace
	#msg += " successfully cloned to " + args.to_project
	#msg += "/" + args.to_workspace
	#print_(msg)


def getGSBucket(p,w):
	#print "find out"
	api_url="https://api.firecloud.org/api"
	r = fapi.list_workspaces(api_url)
	fapi._check_response_code(r, 200)
	#print "r is ",r
	#Parse the JSON for the workspace + namespace
	workspaces = r.json()

	#pretty_spaces = []
	for space in workspaces:
		#print "spsace is ",space
		ns = str(space['workspace']['namespace'])
		ws = str(space['workspace']['name'])
		if(ns==p and ws==w):
			bucketName=str(space['workspace']['bucketName'])
			gsURL="gs://"+bucketName
			return gsURL
	return None


def ws_annot_transfer_and_bucket_rename(p,w,dp,dw):
	sBucket=getGSBucket(p,w)
	dBucket=getGSBucket(dp,dw)	
	r = fapi.get_workspace(p,w,"https://api.firecloud.org/api")
	fapi._check_response_code(r, 200)

	workspace_attrs = r.json()['workspace']['attributes']

	if len(workspace_attrs) == 0:
			print "No workspace attributes defined !"
	else:

		for k in sorted(workspace_attrs.keys()):
			#print "key is ",k
			if(not(k.startswith("library:"))):
				print k + "\t" + str(workspace_attrs[k])
				workspace_attrs[k]=workspace_attrs[k].replace(sBucket,dBucket)
				print "Attempting update for it...."
				update = fapi._attr_set(k,workspace_attrs[k])
				r = fapi.update_workspace_attributes(dp,dw,[update], api_root="https://api.firecloud.org/api")
				r = fapi._check_response_code(r, 200)



def entity_transfer_and_bucket_rename(p,w,dp,dw,access_token):
	sBucket=getGSBucket(p,w)
	dBucket=getGSBucket(dp,dw)
	print "Source bucket is ",sBucket
	print "Destin bucket is ",dBucket
	e_types=["participant","sample","pair","participant_set","sample_set","pair_set"]
	for e_type in e_types:
		print "Downloading TSV for entity type ",e_type+" and performing bucket search-and-replace using curl..."
		tmp_file_name=p+"."+w+"."+e_type+".tsv"
		tmp_file_name_upload=tmp_file_name+".mod.tsv"
		curl_cmd="curl  -o "+tmp_file_name+"  -X GET --header \"Authorization: Bearer "+access_token+"\" "
		curl_cmd=curl_cmd+" \"https://api.firecloud.org/api/workspaces/"+p+"/"+w+"/entities/"+e_type+"/tsv\""
		#print "To run curl command "+curl_cmd
		subprocess.check_output(curl_cmd, shell=True)
		reader=open(tmp_file_name,'r')
		writer=open(tmp_file_name_upload,'w')
		lines_read_this_entity=0
		for line in reader:
			lines_read_this_entity=lines_read_this_entity+1
			line=line.replace(sBucket,dBucket)
			writer.write(line)
		writer.close()

		if(lines_read_this_entity>1):
			#empty TSV upload causes problem, so only upload when necessary
			with open(tmp_file_name_upload) as tsvf:
				headerline = tsvf.readline().strip()
				entity_data = [l.strip() for l in tsvf]
				chunk_size=500
				print "Uploading ",tmp_file_name
				if not fcfiss._batch_load(dp,dw, headerline, entity_data,chunk_size, "https://api.firecloud.org/api", True):
					print "Successfully uploaded entities"
				else:
					print "Error in Bucket replacement!"
					#print_('Error encountered trying to upload entities, quitting....')
					#return 1

		print "\n\n\n"




if __name__ == "__main__":
	parser = ArgumentParser(description="deep copy a workspace. Requires curl, gcloud, and firecloud python bindings")
	parser.add_argument('SOURCE_PROJ',type=str,help="source project")
	parser.add_argument('SOURCE_WS',type=str,help="source workspace")
	parser.add_argument('DEST_PROJ',type=str,help="destination project")
	parser.add_argument('DEST_WS',type=str,help="destination workspace")
	args = parser.parse_args()
	if(args):
		seen=doesUserSeeWorkspace(args.SOURCE_PROJ,args.SOURCE_WS)
		if(seen):
			print "Successfully found source workspace "+args.SOURCE_PROJ+"/"+args.SOURCE_WS
			seen_clone=doesUserSeeWorkspace(args.DEST_PROJ,args.DEST_WS)
			if(seen_clone):
				print "The clone target already exists!"
			else:
				print "Now attempting to clone it to "+args.DEST_PROJ+"/"+args.DEST_WS
				cloneWS(args.SOURCE_PROJ,args.SOURCE_WS,args.DEST_PROJ,args.DEST_WS)
				seen_clone=doesUserSeeWorkspace(args.DEST_PROJ,args.DEST_WS)
				if(seen_clone):
					print "Successfully cloned!"
					accessToken=getAccessToken()
					entity_transfer_and_bucket_rename(args.SOURCE_PROJ,args.SOURCE_WS,args.DEST_PROJ,args.DEST_WS,accessToken)
					ws_annot_transfer_and_bucket_rename(args.SOURCE_PROJ,args.SOURCE_WS,args.DEST_PROJ,args.DEST_WS)
					recursiveBucketCopy(args.SOURCE_PROJ,args.SOURCE_WS,args.DEST_PROJ,args.DEST_WS)
				else:
					print "Error in cloning, could not find clone after cloning operation!"
		else:
			print "Could not find source workspace ",args.SOURCE_PROJ,"/",args.SOURCE_WS
	else:
		parser.print_help()

