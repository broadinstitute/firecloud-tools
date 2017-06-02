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
	print "sBucket is ",sBucket
	print "dBucket is ",dBucket
	bucket_cp_cmd="gsutil -m cp -r "+sBucket+"/* "+dBucket
	print "The copy command to be run is "+bucket_cp_cmd
	output=subprocess.check_output(bucket_cp_cmd, shell=True)
	print "the output is \n*****\n",output,"\n******"
	




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



def entity_transfer_and_bucket_rename(p,w,dp,dw):
	sBucket=getGSBucket(p,w)
	dBucket=getGSBucket(dp,dw)
	print "Source bucket is ",sBucket
	print "Destin bucket is ",dBucket
	e_types=["participant","sample","pair","participant_set","sample_set","pair_set"]
	for e_type in e_types:
		print "analyzing for type ",e_type
		entities = fcfiss._entity_paginator(p, w,e_type,page_size=1000, filter_terms=None,sort_direction="asc", api_root="https://api.firecloud.org/api")
		attr_list = {k for e in entities for k in e['attributes'].keys()}
		attr_list = sorted(attr_list)
		tmp_file_name=p+"."+w+"."+e_type+".tsv"
		writer=open(tmp_file_name,'w')
		header = "entity:"+e_type + "_id\t" + "\t".join(attr_list)
		writer.write(header+"\n")
		for entity_dict in entities:
			print "entity_dict is ",entity_dict
			name = entity_dict['name']
			etype = entity_dict['entityType']
			attrs = entity_dict['attributes']
			print "attributes : ",attrs
			line = name 
			for attr in attr_list:
				##Get attribute value
				if attr == "participant" and (e_type == "sample" or e_type=="pair")  :
					#print "Grab value for part_case"
					value = attrs['participant']['entityName']
					#print "value is ",value
				elif attr=="case_sample" and e_type=="pair":
					#print "Grab value for part_case_case"
					value = attrs['case_sample']['entityName']
					#print "value is ",value
				elif attr=="control_sample" and e_type=="pair":
					#print "Grab value for part_case_case"
					value = attrs['control_sample']['entityName']
					#print "value is ",value
					
				else:
					print "Grab value from attrs "
					value = attrs.get(attr, "")
					print "value is ",value

                # If it's a dict, we get the entity name from the "items" section
                # Otherwise it's a string (either empty or the value of the attribute)
                # so no modifications are needed
				if type(value) == dict:
					#print "VALUE FOR DICT IS ",value
					value = ",".join([i['entityName'] for i in value['items']])
				line += "\t" + value


			writer.write(line.replace(sBucket,dBucket)+"\n")
		writer.close()


		with open(tmp_file_name) as tsvf:
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
	parser = ArgumentParser(description="deep copy a workspace")
	parser.add_argument('SOURCE_PROJ',type=str,help="source project")
	parser.add_argument('SOURCE_WS',type=str,help="source workspace")
	parser.add_argument('DEST_PROJ',type=str,help="source project")
	parser.add_argument('DEST_WS',type=str,help="source workspace")
	args = parser.parse_args()
	if(args):
		print "yes"
		seen=doesUserSeeWorkspace(args.SOURCE_PROJ,args.SOURCE_WS)
		if(seen):
			seen_clone=doesUserSeeWorkspace(args.DEST_PROJ,args.DEST_WS)
			if(seen_clone):
				print "The clone target already exists!"
			else:
				cloneWS(args.SOURCE_PROJ,args.SOURCE_WS,args.DEST_PROJ,args.DEST_WS)
				seen_clone=doesUserSeeWorkspace(args.DEST_PROJ,args.DEST_WS)
				if(seen_clone):
					print "Successfully cloned!"
					entity_transfer_and_bucket_rename(args.SOURCE_PROJ,args.SOURCE_WS,args.DEST_PROJ,args.DEST_WS)
					ws_annot_transfer_and_bucket_rename(args.SOURCE_PROJ,args.SOURCE_WS,args.DEST_PROJ,args.DEST_WS)
					recursiveBucketCopy(args.SOURCE_PROJ,args.SOURCE_WS,args.DEST_PROJ,args.DEST_WS)
				else:
					print "Error in cloning!"
		else:
			print "Could not find source workspace ",args.SOURCE_PROJ,"/",args.SOURCE_WS
	else:
		print "no"

