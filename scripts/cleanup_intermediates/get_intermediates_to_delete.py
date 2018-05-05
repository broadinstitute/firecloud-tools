# make use of the common library
from common import *
from copy import deepcopy
import requests.auth as mymodule
import types
import itertools

def flatten(items, seqtypes=(list, tuple)):
    for i, x in enumerate(items):
        while i < len(items) and isinstance(items[i], seqtypes):
            items[i:i+1] = items[i]
    return items
# def flatten(nested_list):
#     """Flatten an arbitrarily nested list, without recursion (to avoid
#     stack overflows). Returns a new list, the original list is unchanged.
#     >> list(flatten_list([1, 2, 3, [4], [], [[[[[[[[[5]]]]]]]]]]))
#     [1, 2, 3, 4, 5]
#     >> list(flatten_list([[1, 2], 3]))
#     [1, 2, 3]
#     """
#     nested_list = deepcopy(nested_list)
#     
#     while nested_list:
#         sublist = nested_list.pop(0)
# 
#         if isinstance(sublist, list):
#             nested_list = sublist + nested_list
#         else:
#             yield sublist
            
def gather_all_outputs(ws_project, ws_name, sub_id, wf_id):
	if not os.path.exists("/tmp/metadatas"):
		os.makedirs("/tmp/metadatas/")
    
	metadata_filename = "/tmp/metadatas/metadata__%s_%s__%s_%s.json" % (ws_project, ws_name, sub_id, wf_id)
	
	if os.path.exists(metadata_filename):
		metadata = json.load(open(metadata_filename))
	else:
		headers = firecloud_api._fiss_access_headers()
		uri = "https://cromwell1.dsde-prod.broadinstitute.org/api/workflows/v1/{4}/metadata?includeKey=outputs&expandSubWorkflows=true".format(
			get_fc_url(), ws_project, ws_name, sub_id, wf_id)
		metadata = requests.get(uri, headers=headers).json()
		#metadata = get_workflow_metadata(ws_project, ws_name, sub_id, wf_id)
	
	
		with open(metadata_filename, "w") as metadataFile:
			metadataFile.write(json.dumps(metadata))
				
	#print sub_id, wf_id
	mymodule.pb.increment()
	mymodule.pb.print_bar()
	return metadata

def initProcess(pb):
 	mymodule.pb = pb


def main():
	setup()

	# The main argument parser
	parser = DefaultArgsParser(description="Clean up intermediate files from a workspace, a submission in a workspace, or a specific workflow.")

	# Core application arguments
	parser.add_argument('-p', '--project', dest='ws_project', action='store', required=True, help='Workspace project')
	parser.add_argument('-n', '--workspace_name', dest='ws_name', action='store', required=True, help='Workspace name')

	parser.add_argument('-s', '--subid', dest='submission_id', action='store', required=False,
						help='Optional Submission ID to limit the cleanup to.  If not provided this script will clean up intermediates in all submissions in this workspace.')
	parser.add_argument('-w', '--wfid', dest='workflow_id', default=[], action='append', required=False,
						help='Optional Workflow ID(s) to limit the cleanup to.  Note that this requires a submission id to be passed as well."')
	parser.add_argument('-f', '--failed', dest='include_failed', action='store_true', required=False,
						help='Optional flag to include failed and aborted workflows when looking for intermediates to clean up.  By default only succeeded workflows get their intermediates cleaned."')

	
	args = parser.parse_args()

	print "Getting workspace data..."
	workspace_request = firecloud_api.get_workspace(args.ws_project, args.ws_name)
	if workspace_request.status_code != 200:
		fail("Unable to find workspace: %s/%s  at  %s --- %s" % (args.ws_project, args.ws_name, get_fc_url(), workspace_request.text))
	workspace = workspace_request.json()
	workspace_attributes = workspace["workspace"]["attributes"]
	bucketName = workspace["workspace"]["bucketName"]
	print "done.\n"
	
	print "Getting submission data..."
	potential_workflows_to_clean = defaultdict(list)
	if not args.submission_id:
		submissions_request = firecloud_api.list_submissions(args.ws_project, args.ws_name)

		if submissions_request.status_code != 200:
			fail("Unable to list submissions for %s/%s  at  %s" (workspaceProject, workspaceName, get_fc_url()))

		submissions_json = submissions_request.json()
		submission_ids = [s["submissionId"] for s in submissions_json]
	else:
		
		submission_request = firecloud_api.get_submission(args.ws_project, args.ws_name, args.submission_id)
		if submission_request.status_code != 200:
			fail("Unable to get submission info for id: %s" % args.submission_id)
	
		submission_json = submission_request.json()
		submission_ids = [submission_json["submissionId"]]

	statuses = ["Succeeded"] if not args.include_failed else ["Succeeded", "Failed", "Aborted"]
	
	num_workflows_to_process = 0	
	for sub_id in submission_ids:
		workflow_func = lambda w: (
							# if there is no argument given for workflows, or if the workflow id is in the list of workflow ids given 
							# then we want to get metadata for this workflow
							True if not args.workflow_id or (args.workflow_id and w["workflowId"] in args.workflow_id) else False
						)
		filter_func = lambda w: (w["status"] in statuses if "workflowId" in w and workflow_func(w) else False)

		sub_details_json = firecloud_api.get_submission(args.ws_project, args.ws_name, sub_id).json()
		succeeded_workflow_ids = map(lambda w: w["workflowId"], filter(filter_func, sub_details_json["workflows"]))
 		
 		num_workflows_to_process += len(succeeded_workflow_ids)
		potential_workflows_to_clean[sub_id] = succeeded_workflow_ids
	print "done.\n"
	
	
	print "Gathering files to keep..."
	original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
	pb = ProgressBar(0, num_workflows_to_process, "Metadata gathered")
	pool = mp.Pool(initializer=initProcess,initargs=(pb,), processes=5)
	signal.signal(signal.SIGINT, original_sigint_handler)
	
	try:
		# results = [[pool.apply(gather_all_outputs, args=(all_call_outputs, args.ws_project, args.ws_name, sub_id, wf_id))  for wf_id in wf_ids] 
# 						for sub_id, wf_ids in workflows_to_keep.iteritems()]
		metadata = [[pool.apply(gather_all_outputs, args=(args.ws_project, args.ws_name, sub_id, wf_id))  for wf_id in wf_ids] 
						for sub_id, wf_ids in potential_workflows_to_clean.iteritems()]		
		
		#[[r.get() for r in rl] for rl in results]	
		
		files_to_delete = []
		workflow_output_files = []
		call_output_files = []
		
		print "\n"
		metadatas = flatten(metadata)
		pb = ProgressBar(0, num_workflows_to_process, "Metadata processed")
		pb.print_bar()
		for metadata in metadatas:
			if "outputs" in metadata:
				workflow_output_files.extend([v for v in flatten(metadata["outputs"].values()) if str(v).startswith("gs://")])
	
			for call_name, calls in metadata["calls"].iteritems():
				for call in calls:
					if "outputs" in call:
						call_output_files.extend([v for v in flatten(call["outputs"].values()) if str(v).startswith("gs://")])
			pb.increment()
			pb.print_bar()
		
		call_output_files_list = flatten(call_output_files)
		workflow_output_files_list = flatten(workflow_output_files)
		print "\n\tWriting output files..."
		with open("/tmp/files_from_all_calls__%s_%s.txt" % (args.ws_project, args.ws_name), "w") as filesFromAllCalls:
			for f in call_output_files_list:
				filesFromAllCalls.write(f+"\n")
				
		with open("/tmp/files_from_workflow_outputs__%s_%s.txt" % (args.ws_project, args.ws_name), "w") as filesFromAllCalls:
			for f in workflow_output_files_list:
				filesFromAllCalls.write(f+"\n")

		files_to_delete = set(call_output_files_list) - set(workflow_output_files_list)
		files_to_delete_path = "/tmp/files_to_delete__%s_%s.txt" % (args.ws_project, args.ws_name)
		with open(files_to_delete_path, "w") as filesToDelete:
			for f in files_to_delete:
				filesToDelete.write(f+"\n")
		print "\tdone."
		
	except KeyboardInterrupt:
		pool.terminate()
	else:
		pool.close()
	pool.join()
	print "\ndone.\n"
	
	print "Files to delete can be found in: %s" % os.path.abspath(files_to_delete_path)
	print "This file can be passed to the delete.py script in order to remove these intermediates from their bucket."
	
if __name__ == "__main__":
	main()
