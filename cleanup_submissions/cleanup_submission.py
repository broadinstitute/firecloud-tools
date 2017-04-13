##!/usr/bin/env python
from common import *

def find_submissions_in(namespace, name):
    print "\tGetting submissions for workspace: %s/%s..." % (namespace, name)
        
    result = firecloud_api.list_submissions(namespace, name, fc_url)
    if result.status_code == 200:
        return result.json()

def list_objects_in(bucket, folder):
    """Returns a list of metadata of the objects within the given bucket."""
    service = create_service()

    # Create a request to objects.list to retrieve a list of objects.
    fields_to_return = \
        'nextPageToken,items(name,size,contentType,metadata(my-key))'
    req = service.objects().list(bucket=bucket, prefix=folder, fields=fields_to_return)

    all_objects = {}
    # If you have too many items to list in one request, list_next() will
    # automatically handle paging with the pageToken.
    while req:
        resp = req.execute()
        items = resp.get('items', [])

        for item in items:
            full_path = "gs://%s/%s" % (bucket, item["name"])
            all_objects[full_path] = item
            
        req = service.objects().list_next(req, resp)
    return all_objects

def remove_object(bucket, object):
    service = create_service()

    req = service.objects().delete(bucket=bucket, object=object)
    resp = req.execute()

    return resp
    
# get first workflow - get outputs - are all outputs present even if no output was produced?
# list all bucket items, show every output file, list out ones that are not outputs
# show one list of files for all workflows with <workflow_id> shown

    
def find_files_to_clean_in(ws_namespace, ws_name, submission_id, ignored_file_keywords):
    workspace_request = firecloud_api.get_workspace(ws_namespace, ws_name, fc_url)
    
    if workspace_request.status_code != 200:
        fail("Unable to find workspace: %s/%s  at  %s --- %s" % (ws_namespace, ws_name, fc_url, workspace_request.text))
        
    workspace = workspace_request.json()
    bucketName = workspace["workspace"]["bucketName"]
    
    print "Retrieving submission..."
    sub_json = firecloud_api.get_submission(ws_namespace, ws_name, submission_id, fc_url).json()
    
    #mc_ns = sub_json["methodConfigurationNamespace"]
    #mc_name = sub_json["methodConfigurationName"]
    
    
    workflow_outputs = []
    longest_name = 0
    workflows = sub_json["workflows"]
    workflow_id_to_files = defaultdict(list)
    
    # look at first workflow
    first_workflow = workflows[0]
    workflow_id = first_workflow["workflowId"]
    workflow_meta = get_workflow_metadata(ws_namespace, ws_name, submission_id, workflow_id)
    outputs = workflow_meta["outputs"]
    workflow_name = workflow_meta["workflowName"]
    
    unexpected_files = []
    unique_file_patterns = defaultdict(list)
    
    workflow_call_folder_re = r"%s/[^/]*/" % (workflow_name)
    workflow_log_re = r"workflow.logs/workflow.[^/]*.log"
    
    print "Retrieving bucket listing..."    
    bucket_objects = list_objects_in(bucketName, submission_id)
    
    print "Processing bucket listing..."
    for obj in bucket_objects:
        obj_name = obj["name"]
        full_path = "gs://%s/%s" % (bucketName, obj_name)
        sub_object_name = obj_name.replace("%s/" % submission_id, "")
        
        path_parts = sub_object_name.split("/")
        first_folder = path_parts[0]
        
        if re.match(workflow_call_folder_re, sub_object_name):
            call_pattern = re.sub(workflow_call_folder_re, "%s/<WORKFLOW_ID>/"%workflow_name, sub_object_name)
            unique_file_patterns[call_pattern].append(obj)
        elif re.match(workflow_log_re, sub_object_name):
            unique_file_patterns["workflow.logs/workflow.<WORKFLOW_ID>.log"].append(obj)
        
        workflow_id = None
        if first_folder == workflow_name:
            workflow_id = path_parts[1]
        elif first_folder == "workflow.logs":
            workflow_id = path_parts[1].replace("workflow.","").replace(".log", "")
        else:
            unexpected_files.append(full_path)
    
        workflow_id_to_files[workflow_id].append(full_path) 
    
    # for key in workflow_id_to_files.keys():
#         print key
#         for f in workflow_id_to_files[key]:
#             print "\t", f
#     
    print "\nFiles not accounted for:"
    print unexpected_files
    
    unique_file_patterns_to_delete = defaultdict(list)
    unique_file_patterns_to_ignore = defaultdict(list)
    
    longest_unique_pattern = max(len(p) for p in unique_file_patterns)
    
    for p in unique_file_patterns:
        # get just the file name by matching on the characters after / at the end of the path
        file_name = re.findall(r"[^/]*$", p)[0]
        
        if any(re.match(ignored.replace("*", "[^/]*"), file_name) for ignored in ignored_file_keywords):
            unique_file_patterns_to_ignore[p] = unique_file_patterns[p]
        else:
            unique_file_patterns_to_delete[p] = unique_file_patterns[p]
                
    print "\nUnique file patterns to ignore:"
    print_files_table(longest_unique_pattern, unique_file_patterns_to_ignore)
    
    print "\nUnique file patterns to delete:"
    print_files_table(longest_unique_pattern, unique_file_patterns_to_delete)
    
def print_files_table(name_padding, unique_file_patterns):
    if len(unique_file_patterns) == 0:
        print "\tNone."
        return
    
    
    header = "%s%s%s%s" % ("Name".ljust(name_padding+10),"Count".ljust(10),"Avg Size".ljust(10), "Max Size".ljust(10))
    print "\t",header
    print "\t","-"*len(header)
    
    total_file_sizes = 0
    for unique_pattern in sorted(unique_file_patterns. iterkeys()):
        google_objs_for_pattern = unique_file_patterns[unique_pattern]
        pattern_total_size = sum(int(obj['size']) for obj in google_objs_for_pattern)
        total_file_sizes += pattern_total_size
        avg_size = pattern_total_size / len(google_objs_for_pattern)
        max_size = max(int(obj['size']) for obj in google_objs_for_pattern)
        
        print "\t%s%s%s%s" % (unique_pattern.ljust(name_padding+10),str(len(google_objs_for_pattern)).ljust(10),human_file_size_fmt(avg_size).ljust(10), human_file_size_fmt(max_size).ljust(10))
    print "\t","-"*len(header)
    print "\tTotal File Size: %s" % human_file_size_fmt(total_file_sizes)

def get_metadata_parallel(ws_namespace, ws_name, submission_id, workflow_id):
    workflow_meta = get_workflow_metadata(ws_namespace, ws_name, submission_id, workflow_id)           
    
    return workflow_meta
                
def find_files_to_clean_in_old(ws_namespace, ws_name, submission_id, ignored_file_keywords):
    workspace_request = firecloud_api.get_workspace(ws_namespace, ws_name)
    
    if workspace_request.status_code != 200:
        fail("Unable to find workspace: %s/%s  at  %s --- %s" % (ws_namespace, ws_name, firecloud_api.PROD_API_ROOT, workspace_request.text))
        
    workspace = workspace_request.json()
    bucketName = workspace["workspace"]["bucketName"]
    
    
    print "Retrieving submission..."
    sub_json = firecloud_api.get_submission(ws_namespace, ws_name, submission_id).json()

    status = sub_json["status"]

    if status not in ("Done", "Aborted", "Failed"):
        fail("The submission provided has not yet completed.")
    
    mc_ns = sub_json["methodConfigurationNamespace"]
    mc_name = sub_json["methodConfigurationName"]
    
    print "Getting info on files in bucket..."
    bucket_objects = list_objects_in(bucketName, submission_id)
    
    longest_name = 0
    workflows = sub_json["workflows"]
    
    pool = mp.Pool(processes=20)
    
    num_workflows = len(workflows)
    workflow_idx = 1
    
    metadata_requests = [pool.apply_async(get_metadata_parallel, args=(ws_namespace, ws_name, submission_id, wf["workflowId"])) for wf in workflows]
    metadata_results = [result.get(timeout=10) for result in metadata_requests]
    
    print "Processing workflow metadata..."
    unique_file_patterns = defaultdict(list)
    
    workflow_id_to_outputs = defaultdict(list)
    output_key_to_outputs = defaultdict(list)
    for workflow_meta in metadata_results:
        outputs = workflow_meta["outputs"]
        workflow_id = workflow_meta["id"]
        workflow_name = workflow_meta["workflowName"]
        
        print "\tGathering outputs for workflow: %d/%d --- %s" % (workflow_idx, num_workflows, workflow_id)
        
        for output_name in outputs.keys():
            output = outputs[output_name]

            if re.match("gs://.*", str(output)):
                workflow_id_to_outputs[workflow_id].append({"output_name": output_name, "output_url": output}) 
                output_key_to_outputs[output_name].append(output)
        workflow_idx += 1
    
    pool.close()
    pool.join()
    
    first_workflow_id_to_keep = workflow_id_to_outputs.keys()[0]
    print "Example workflow outputs -- %s:" % first_workflow_id_to_keep
    for output in workflow_id_to_outputs[first_workflow_id_to_keep]:
        print output["output_name"], output["output"]
    
    
    # for file in bucket_objects:
    #     output_file_info = bucket_objects[file]
    #     workflow_call_folder_re = r"%s/[^/]*/" % (workflow_name)
    #     workflow_log_re = r"workflow.logs/workflow.[^/]*.log"
    #
    #     sub_object_name = file.replace("gs://%s/%s/" % (bucketName, submission_id), "")
    #     path_parts = sub_object_name.split("/")
    #     first_folder = path_parts[0]
    #
    #     if re.match(workflow_call_folder_re, sub_object_name):
    #         call_pattern = re.sub(workflow_call_folder_re, "", sub_object_name)
    #         unique_file_patterns[call_pattern].append(output_file_info)
    #
    #     elif re.match(workflow_log_re, sub_object_name):
    #         unique_file_patterns["workflow.logs/workflow.<WORKFLOW_ID>.log"].append(output_file_info)
    #
    #
    #
    #
    # unique_file_patterns_to_keep = defaultdict(list)
    # unique_file_patterns_to_delete = defaultdict(list)
    # unique_file_patterns_to_ignore = defaultdict(list)
    #
    # longest_unique_pattern = max(len(p) for p in unique_file_patterns)
    #
    # for p in unique_file_patterns:
    #     # get just the file name by matching on the characters after / at the end of the path
    #     file_name = re.findall(r"[^/]*$", p)[0]
    #
    #     if unique_file_patterns[p][0]['name'] in workflow_outputs:
    #         unique_file_patterns_to_keep[p] = unique_file_patterns[p]
    #     elif any(re.match(ignored.replace("*", "[^/]*"), file_name) for ignored in ignored_file_keywords):
    #         unique_file_patterns_to_ignore[p] = unique_file_patterns[p]
    #     else:
    #         unique_file_patterns_to_delete[p] = unique_file_patterns[p]
    #
    # print "\nUnique file patterns to keep:"
    # print_files_table(longest_unique_pattern, unique_file_patterns_to_keep)
    #
    # print "\nUnique file patterns to ignore:"
    # print_files_table(longest_unique_pattern, unique_file_patterns_to_ignore)
    #
    # print "\nUnique file patterns to delete:"
    # print_files_table(longest_unique_pattern, unique_file_patterns_to_delete)

                #print "Files to keep from workflow outputs:"
            #     for output in workflow_outputs:
            #         #print "\t%s: %s" % (output[0].ljust(longest_name), output[1])
            #         print "\t%s:\n\t\t%s\n" % (output[0].ljust(longest_name), output[1])



                # workflow_outputs = []
            #     longest_name = 0
            #     workflows = sub_json["workflows"]
            #     workflow_id_to_files = defaultdict(list)
            #
            #     # look at first workflow
            #     first_workflow = workflows[0]
            #     workflow_id = first_workflow["workflowId"]
            #     workflow_meta = get_workflow_metadata(ws_namespace, ws_name, submission_id, workflow_id)
            #     outputs = workflow_meta["outputs"]
            #     workflow_name = workflow_meta["workflowName"]
            #
            #     unexpected_files = []
            #     unique_file_patterns = defaultdict(list)
            #
            #     workflow_call_folder_re = r"%s/[^/]*/" % (workflow_name)
            #     workflow_log_re = r"workflow.logs/workflow.[^/]*.log"
            #
            #     print "Retrieving bucket listing..."
            #     bucket_objects = list_objects_in(bucketName, submission_id)
            #
            #     print "Processing bucket listing..."
            #     for obj in bucket_objects:
            #         obj_name = obj["name"]
            #         full_path = "gs://%s/%s" % (bucketName, obj_name)
            #         sub_object_name = obj_name.replace("%s/" % submission_id, "")
            #
            #         path_parts = sub_object_name.split("/")
            #         first_folder = path_parts[0]
            #
            #         if re.match(workflow_call_folder_re, sub_object_name):
            #             call_pattern = re.sub(workflow_call_folder_re, "%s/<WORKFLOW_ID>/"%workflow_name, sub_object_name)
            #             unique_file_patterns[call_pattern].append(obj)
            #         elif re.match(workflow_log_re, sub_object_name):
            #             unique_file_patterns["workflow.logs/workflow.<WORKFLOW_ID>.log"].append(obj)
            #
            #         workflow_id = None
            #         if first_folder == workflow_name:
            #             workflow_id = path_parts[1]
            #         elif first_folder == "workflow.logs":
            #             workflow_id = path_parts[1].replace("workflow.","").replace(".log", "")
            #         else:
            #             unexpected_files.append(full_path)
            #
            #         workflow_id_to_files[workflow_id].append(full_path)
            #
            #     # for key in workflow_id_to_files.keys():
            # #         print key
            # #         for f in workflow_id_to_files[key]:
            # #             print "\t", f
            # #
            #     print "\nFiles not accounted for:"
            #     print unexpected_files
            #
            #     unique_file_patterns_to_delete = defaultdict(list)
            #     unique_file_patterns_to_ignore = defaultdict(list)
            #
            #     longest_unique_pattern = max(len(p) for p in unique_file_patterns)
            #
            #     for p in unique_file_patterns:
            #         # get just the file name by matching on the characters after / at the end of the path
            #         file_name = re.findall(r"[^/]*$", p)[0]
            #
            #         if any(re.match(ignored.replace("*", "[^/]*"), file_name) for ignored in ignored_file_keywords):
            #             unique_file_patterns_to_ignore[p] = unique_file_patterns[p]
            #         else:
            #             unique_file_patterns_to_delete[p] = unique_file_patterns[p]
            #
            #     print "\nUnique file patterns to ignore:"
            #     print_files_table(longest_unique_pattern, unique_file_patterns_to_ignore)
            #
            #     print "\nUnique file patterns to delete:"
            #     print_files_table(longest_unique_pattern, unique_file_patterns_to_delete)

                # files_to_delete = []
            #     files_to_ignore = []
            #     bucket_objects = list_objects_in(bucketName, submission_id)
            #     for obj in bucket_objects:
            #         obj_name = obj["name"]
            #         full_path = "gs://%s/%s" % (bucketName, obj_name)
            #
            #         # get just the file name by matching on the characters after / at the end of the path
            #         file_name = re.findall(r"[^/]*$", obj_name)[0]
            #         if full_path not in workflow_outputs:
            #             # if the file_name matches any of the keywords using wildcard search, then ignore it
            #             if any(re.match(ignored.replace("*", "[^/]*"), file_name) for ignored in ignored_file_keywords):
            #                 #re.match(r"" % ignored_files, file_name):
            #                 files_to_ignore.append(full_path)
            #             else:
            #                 files_to_delete.append(full_path)
            #
            #
            #     print "\nFiles that were ignored using keywords - %s:" % ignored_file_keywords
            #     for ignore in files_to_ignore:
            #         print "\t",ignore
            #
            #     if len(files_to_delete) > 0:
            #         print "\n\n"
            #         banner_width = 200
            #         print "=" * banner_width
            #         print "|\tFILES TO BE DELETED".ljust(banner_width-8), "|"
            #         print "=" * banner_width
            #
            #
            #         for delete in files_to_delete:
            #             print "\t",delete
            #
            #
            #         if prompt_to_continue("Are you sure you want to delete these files?"):
            #             for delete in files_to_delete:
            #                 object_name = delete.replace("gs://%s/" % bucketName, "")
            #                 print "deleting %s...%s" % (delete, remove_object(bucketName, object_name))
            #         else:
            #             print "Exiting."
            #     else:
            #         print "\n\nThis submission produced no output files, exiting."


def main():
    setup()
    
    # The main argument parser
    parser = DefaultArgsParser(description="Clean up files from a given submission.")

    # Core application arguments
    parser.add_argument('-p', '--namespace', dest='ws_namespace', action='store', required=True, help='Workspace namespace')
    parser.add_argument('-n', '--name', dest='ws_name', action='store', required=True, help='Workspace name')
    parser.add_argument('-s', '--subid', dest='sub_id', action='store', required=True, help='Submission ID')
    parser.add_argument('-i', '--ignore', dest='ignore_files', default=[], action='append', required=False, help='Filenames to ignore from the set of files to delete, e.g. "exec.sh".  Wildcards are allowed, e.g. "*stdout.log" will match "OncotatorTask-stdout.log" and "MutectTask-stdout.log"')

    
    # Call the appropriate function for the given subcommand, passing in the parsed program arguments
    args = parser.parse_args()
     
    find_files_to_clean_in_old(args.ws_namespace, args.ws_name, args.sub_id, args.ignore_files)

if __name__ == "__main__":
    main()
