workflow cleanup {
  File svcActKeyJson
  String workspaceProject
  String workspaceName
  Array[String] submissionIds
  Boolean useSubmissionIds = length(submissionIds) != 0
  Boolean dryRun

  if (!useSubmissionIds) {
  	call getSubmissionsInWorkspace {input: svcActKeyJson=svcActKeyJson,
  								      workspaceProject=workspaceProject,
  								      workspaceName=workspaceName
  							  }
  }

  Array[String] submissionIdsToClean = select_first([getSubmissionsInWorkspace.submissionIds, submissionIds])

  
  call getFilesOfSubmissions { input: svcActKeyJson=svcActKeyJson,
  							      	workspaceProject=workspaceProject,
  							      	workspaceName=workspaceName,
  							      	submissionIds=submissionIdsToClean 
  							}
  if (!dryRun) {
  	scatter (fofn in getFilesOfSubmissions.filesToDelete ) {
  		call httpBatchDelete { input: fofn=fofn, 
  		                           bucketName=getFilesOfSubmissions.bucketName, 
  		                           svcActKeyJson=svcActKeyJson 
  		}
  	}
  }

  output {
  	Array[File] filesToDelete = getFilesOfSubmissions.filesToDelete
  }
}

task getSubmissionsInWorkspace {
	File svcActKeyJson
	String workspaceProject
    String workspaceName

    command {
gcloud auth activate-service-account --key-file=${svcActKeyJson}
export GOOGLE_APPLICATION_CREDENTIALS=${svcActKeyJson}

python <<CODE
# make use of the common library
from common import *

credentials = GoogleCredentials.get_application_default()
print "Using Google client id:", credentials.client_id

submissions_request = firecloud_api.list_submissions('${workspaceProject}', '${workspaceName}')

if submissions_request.status_code != 200:
	fail("Unable to list submissions for %s/%s  at  %s" (workspaceProject, workspaceName, get_fc_url()))

submissions_json = submissions_request.json()
terminal_submission_ids = map(lambda s: s["submissionId"], filter(lambda s: s["status"] in ["Aborted", "Done"], submissions_json))

with open('submissions_to_clean.txt', 'w') as out_file:
	out_file.write('\n'.join(terminal_submission_ids))
CODE
	}

	output {
		Array[String] submissionIds = read_lines("submissions_to_clean.txt")
	}
	
	runtime {
		docker: "broadinstitute/firecloud-tools"
    	memory: 1
    	disks: "local-disk 1 HDD"
	}


}

task getFilesOfSubmissions {
   File svcActKeyJson
   String workspaceProject
   String workspaceName
   Array[String] submissionIds

command {

gcloud auth activate-service-account --key-file=${svcActKeyJson}
export GOOGLE_APPLICATION_CREDENTIALS=${svcActKeyJson}
    
python <<CODE
    # setup so we can import from the /scripts directory from the firecloud-tools repo so we can
    # make use of the common library
from common import *    
workspaceProject = "${workspaceProject}"
workspaceName = "${workspaceName}"


credentials = GoogleCredentials.get_application_default()
print "Using Google client id:", credentials.client_id

workspace_request = firecloud_api.get_workspace(workspaceProject, workspaceName)
 
if workspace_request.status_code != 200:
	fail("Unable to find workspace: %s/%s  at  %s --- %s" % (workspaceProject, workspaceName, get_fc_url(), workspace_request.text))
 
workspace = workspace_request.json()
workspace_attributes = workspace["workspace"]["attributes"]

bucketName = workspace["workspace"]["bucketName"]
with open('bucket_name.txt', 'w') as out_file:
	out_file.write(bucketName)

entity_types_json = firecloud_api.list_entity_types(workspaceProject, workspaceName).json()
entities_json = []
for entity_type in entity_types_json:
	entities_json.extend(firecloud_api.get_entities(workspaceProject, workspaceName, entity_type).json())


submissionIds = ['${sep="','" submissionIds}']
with open("files_to_delete.txt", "w") as filesToDelete:
	workflows_to_keep = []
	for submissionId in submissionIds:
		submission_json = firecloud_api.get_submission(workspaceProject, workspaceName, submissionId).json()
		workflows_json = submission_json["workflows"]
		workflows_with_id = [workflow_json["workflowId"] for workflow_json in workflows_json if "workflowId" in workflow_json]
		 
		for workflow_id in workflows_with_id:
			# look at workspace attributes to see if any output from a workflow in this submission is bound there
			for key, attribute in workspace_attributes.iteritems():
				if workflow_id in str(attribute):
					print "keeping workflow (%s) - output used in workspace attribute: %s -> %s" %(workflow_id, key, attribute)
					workflows_to_keep.append(workflow_id)
					break
		   
			# look at entity attributes to see if any output from a workflow in this submission is bound there 
			for entity_json in entities_json:
				entity_name = entity_json["name"]
				for key, attribute_value in entity_json["attributes"].iteritems():
					if re.match(r"gs://%s/.*" % bucketName, str(attribute_value)):
						if workflow_id not in workflows_to_keep and workflow_id in attribute_value:
							print "keeping workflow (%s) - output used in entity attribute: %s -> %s" %(workflow_id, key, attribute_value)
							workflows_to_keep.append(workflow_id)
							break
		                     
		for workflow_id in workflows_with_id:
			if workflow_id not in workflows_to_keep:
				print "workflow %s will be cleaned up" % workflow_id
		 
		 
		service = googleapiclient.discovery.build('storage', 'v1')
		fields_to_return = 'nextPageToken,items(name,size)'  # ,contentType,metadata(my-key))'
		    
		req = service.objects().list(bucket=bucketName, prefix=submissionId, fields=fields_to_return)
		
		def ignored_files(call_alias, index):
			ignored_file_extensions = [".log", "-rc.txt", "-stderr.log", "-stdout.log"]
			if not index:
				file_names = map(lambda x: call_alias+x, ignored_file_extensions)
			else:
				file_names = map(lambda x: call_alias+"-"+index+x, ignored_file_extensions)
			return ["exec.sh"] + file_names
		
		total = 0
		while req:
			resp = req.execute()
			items = resp.get('items', [])
			for item in items:
				object_name = item["name"]
				# Needed for workflow logs. gs://fc-<workspace-id>/<submission-id>/workflow.logs/workflow.<workflow-id>.log
				workflow_id = object_name.split("/")[2].replace("workflow.", "").replace(".log", "")
				file_name = re.findall(r"[^/]*$", object_name)[0]
				call_directory = re.findall('call-(.*?)/', object_name)
				shard = re.findall('shard-(.*?)/', object_name)
				file_to_ignore = False
				if call_directory:
					call_alias = call_directory[0]
					shard_index = shard[0] if shard else None
					file_to_ignore = file_name in ignored_files(call_alias, shard_index)
				elif "workflow.logs" in object_name:
					file_to_ignore = True
				workflow_to_keep = workflow_id in workflows_to_keep
				if not workflow_to_keep and not file_to_ignore:
					full_path = "gs://%s/%s" % (bucketName, object_name)
					filesToDelete.write(full_path + '\n')
					print full_path
					total+=1
			req = service.objects().list_next(req, resp)
		print "total number of files to be cleaned up:", total
CODE

split -l 100000 files_to_delete.txt del-

}
    runtime {
   		docker: "broadinstitute/firecloud-tools"
    	memory: 1
    	disks: "local-disk 1 HDD"
    }
    output {
    	String bucketName = read_string("bucket_name.txt")
    	Array[File] filesToDelete = glob("del-*")
    }
}

task httpBatchDelete {
	
	File fofn
	String bucketName
	File svcActKeyJson

	command {

	gcloud auth activate-service-account --key-file=${svcActKeyJson}
    export GOOGLE_APPLICATION_CREDENTIALS=${svcActKeyJson}

	python <<CODE
	import json
	from apiclient.discovery import build
	from apiclient.http import BatchHttpRequest
	
	service = build('storage', 'v1')
	collection = service.objects()
	
	with open('${fofn}', 'rU') as in_file:
		list_blobs = in_file.read().splitlines()
	
	list_objects = map(lambda x: x.replace('gs://${bucketName}/',''), list_blobs)
	n=1000
	chunks=[list_objects[i:i + n] for i in xrange(0, len(list_objects), n)]
	
	# TODO: Needs better error handling for 502 Gateway Errors
	for object_names in chunks:
		batch_delete_requests = service.new_batch_http_request()
		for name in object_names:
			batch_delete_requests.add(collection.delete(bucket="${bucketName}", object=name))
		batch_delete_requests.execute()
	CODE
	}

	runtime {
		docker: "broadinstitute/firecloud-tools"
		disks: "local-disk 1 HDD"
	}
}
