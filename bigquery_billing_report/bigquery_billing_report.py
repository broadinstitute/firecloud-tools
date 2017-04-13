##!/usr/bin/env python
from common import *


def find_submissions_in(namespace, name):
    print "\tGetting submissions for workspace: %s/%s..." % (namespace, name)
        
    result = firecloud_api.list_submissions(namespace, name)
    if result.status_code == 200:
        return result.json()

def list_objects_in(bucket , *folders):
    """Returns a list of metadata of the objects within the given bucket."""
    service = create_service()

    # Create a request to objects.list to retrieve a list of objects.
    fields_to_return = \
        'nextPageToken,items(name,size,contentType,metadata(my-key))'
    req = service.objects().list(bucket=bucket, fields=fields_to_return)

    all_objects = []
    # If you have too many items to list in one request, list_next() will
    # automatically handle paging with the pageToken.
    while req:
        resp = req.execute()
        items = resp.get('items', [])
        
        # if folders were passed along with bucket, then only return objects in those folders
        if folders:
            filtered_items = []
            for item in items:
                # split item on slashes to get "folders"
                item_objects = item["name"].split("/")
                
                # if this items first "folders" match the passed folders, then use include this object
                if list(item_objects[0:len(folders)]) == list(folders):
                    filtered_items.append(item)
            items = filtered_items
            
        all_objects.extend(items)
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
    workspace_request = firecloud_api.get_workspace(ws_namespace, ws_name)
    
    if workspace_request.status_code != 200:
        fail("Unable to find workspace: %s/%s  at  %s --- %s" % (ws_namespace, ws_name, workspace_request.text))
        
    workspace = workspace_request.json()
    bucketName = workspace["workspace"]["bucketName"]
    
    print "Retrieving submission..."
    sub_json = firecloud_api.get_submission(ws_namespace, ws_name, submission_id).json()
    
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
    
    for unique_pattern in unique_file_patterns.keys():
        google_objs_for_pattern = unique_file_patterns[unique_pattern]
        avg_size = sum(int(obj['size']) for obj in google_objs_for_pattern) / len(google_objs_for_pattern)
        max_size = max(int(obj['size']) for obj in google_objs_for_pattern)
        print "\t%s%s%s%s" % (unique_pattern.ljust(name_padding+10),str(len(google_objs_for_pattern)).ljust(10),human_file_size_fmt(avg_size).ljust(10), human_file_size_fmt(max_size).ljust(10))

def get_pricing(ws_namespace, ws_name, query_sub_id = None, query_workflow_id = None):
    print "Retrieving submissions in workspace..."       
    workspace_request = firecloud_api.get_workspace(ws_namespace, ws_name)
    
    if workspace_request.status_code != 200:
        fail("Unable to find workspace: %s/%s  at  %s --- %s" % (ws_namespace, ws_name, workspace_request.text))
        
    workspace = workspace_request.json()
    bucketName = workspace["workspace"]["bucketName"]
    
    submissions_json = firecloud_api.list_submissions(ws_namespace, ws_name).json()
    
    workflow_dict = {}
    for submission_json in submissions_json:
        sub_id = submission_json["submissionId"]
        
        if query_sub_id and sub_id not in query_sub_id:
            continue;
        
        sub_details_json = firecloud_api.get_submission(ws_namespace, ws_name, sub_id).json()
        for wf in sub_details_json["workflows"]:
            wf_id = wf["workflowId"]
            if query_workflow_id and wf_id not in query_workflow_id:
                continue;

            workflow_dict[wf_id] = {"submission_id":sub_id, "workflow":wf}
    
    get_workflow_pricing(ws_namespace, ws_name, workflow_dict)
                
    # print "Retrieving submission..."
#     sub_json = firecloud_api.get_submission(ws_namespace, ws_name, submission_id).json()
#     
#     #mc_ns = sub_json["methodConfigurationNamespace"]
#     #mc_name = sub_json["methodConfigurationName"]
#     
#     
#     workflow_outputs = []
#     longest_name = 0
#     workflows = sub_json["workflows"]
#     
#     num_workflows = len(workflows)
#     workflow_idx = 1
#     for wf in workflows:
#         workflow_id = wf["workflowId"]
#         print "\tProcessing workflow: %d/%d --- %s" % (workflow_idx, num_workflows, workflow_id)
#         workflow_idx += 1
#         
#         workflow_meta = get_workflow_metadata(ws_namespace, ws_name, submission_id, workflow_id)
#                 
#         outputs = workflow_meta["outputs"]
#         #longest = len(max(outputs.keys(), key=len))
#         #longest_name = max(longest_name, longest)
#         
#         for output_name in outputs.keys():
#             output = outputs[output_name]
#             if re.match("gs://.*", output):
#                 workflow_outputs.append((output_name, output))
#     
#     print "Files to keep from workflow outputs:"
#     for output in workflow_outputs:
#         #print "\t%s: %s" % (output[0].ljust(longest_name), output[1])
#         print "\t%s:\n\t\t%s\n" % (output[0].ljust(longest_name), output[1])


#def get_pricing_for_submission(ws_namespace, ws_name, submission_id = None): 
    
class CostRow():
    def __init__(self, cost, product, resource_type, workflow_id, task_name, call_name):
        self.cost = cost
        self.product = product 
        self.resource_type = resource_type
        self.workflow_id = workflow_id
        self.task_name = task_name
        self.call_name = call_name
    
    
                      
def get_workflow_pricing(ws_namespace, ws_name, workflow_dict):
    if len(workflow_dict) == 0:
        fail("No submissions or workflows matching the criteria were found.")

    # Imports the Google Cloud client library
    from google.cloud import bigquery

    #SELECT project.id, GROUP_CONCAT(labels.key) WITHIN RECORD labels_key, GROUP_CONCAT(labels.value) WITHIN RECORD labels_value, cost, product, resource_type
    #FROM [broad-dsde-cromwell-dev:billing_export.gcp_billing_export_00EE8D_A5E267_DF8E43] 
    #WHERE project.id = '%s' AND labels.key IN ("cromwell-workflow-id", "cromwell-workflow-name", "cromwell-sub-workflow-name", "wdl-task-name", "wdl-call-alias")
    
    #todo: one query per submission and show the results per submission
    #todo: cost per workflow, cost per submission, cost per workspace
    
    subquery_template = "labels_value LIKE \"%%%s%%\""
    workflows_subquery = " OR ".join([subquery_template % workflow_id for workflow_id in workflow_dict])
    
    print "Gathering pricing data..."

    client = bigquery.Client(ws_namespace)
    dataset = client.dataset('billing_export')
    for table in dataset.list_tables():
        query = """
              SELECT GROUP_CONCAT(labels.key) WITHIN RECORD AS labels_key, 
                     GROUP_CONCAT(labels.value) WITHIN RECORD labels_value, 
                     cost, 
                     product, 
                     resource_type
              FROM %s.%s
              WHERE project.id = '%s' 
                      AND 
                    labels.key IN ("cromwell-workflow-id", 
                                   "cromwell-workflow-name", 
                                   "cromwell-sub-workflow-name", 
                                   "wdl-task-name", 
                                   "wdl-call-alias")
              HAVING 
                 %s
            """ % (dataset.name, table.name, ws_namespace, workflows_subquery)


        #print query

        # [END create_client]
        # [START run_query]
        query_results = client.run_sync_query(query)

        # Use standard SQL syntax for queries.
        # See: https://cloud.google.com/bigquery/sql-reference/
        #query_results.use_legacy_sql = False

        query_results.run()

        print "Processing data..."
        # [END run_query]

        # [START print_results]
        # Drain the query results by requesting a page at a time.
        page_token = None


        workflow_id_to_cost = defaultdict(list)
        longest_task_name_len = 0
        while True:
            rows, total_rows, page_token = query_results.fetch_data(
                max_results=1000,
                page_token=page_token)
            for row in rows:
                labels = dict(zip(row[0].split(","), row[1].split(",")))
                cost = row[2]
                product = row[3]
                resource_type = row[4]

                workflow_id = labels["cromwell-workflow-id"].replace("cromwell-", "")
                task_name = labels["wdl-task-name"]
                if "wdl-call-alias" not in labels:
                    call_name = task_name
                else:
                    call_name = labels["wdl-call-alias"]
                #longest_task_name_len = max(len(call_name), longest_task_name_len)
                workflow_id_to_cost[workflow_id].append(CostRow(cost, product, resource_type, workflow_id, task_name, call_name) )
                #print project_id, labels, cost, product, resource_type

            if not page_token:
                break
        # [END print_results]

        for wf_id in workflow_id_to_cost:
            submission_id = workflow_dict[wf_id]["submission_id"]
            workflow_json = workflow_dict[wf_id]["workflow"]
            workflow_metadata_json = get_workflow_metadata(ws_namespace, ws_name, submission_id, wf_id)
            calls_lower_json = dict((k.split(".")[-1].lower(), v) for k, v in workflow_metadata_json["calls"].iteritems())

            print "Submission: %s | Workflow: %s (%s)" % (submission_id, wf_id, workflow_json["status"])


            call_name_to_cost = defaultdict(list)
            for c in workflow_id_to_cost[wf_id]:
                call_name_to_cost[c.call_name].append(c)


            total = 0.0
            pd_cost = 0.0
            cpu_cost = 0.0
            other_cost = 0.0
            for call_name in call_name_to_cost:
                call_pricing = call_name_to_cost[call_name]


                resource_type_to_pricing = defaultdict(int)
                for pricing in call_pricing:
                    resource_type_to_pricing[pricing.resource_type] += pricing.cost

                num_calls = len(calls_lower_json[call_name]) if call_name in calls_lower_json else 0
                print "\n\t%-10s%s:" % ("(%dx)" % num_calls, call_name)

                sorted_costs = sorted(resource_type_to_pricing, key=lambda rt: resource_type_to_pricing[rt], reverse=True)
                for resource_type in sorted_costs:
                    cost = resource_type_to_pricing[resource_type]
                    if cost > 0:
                        total += cost
                        if "pd" in resource_type.lower():
                            pd_cost += cost
                        elif "cpu" in resource_type.lower():
                            cpu_cost += cost
                        else:
                            other_cost += cost

                        print "\t\t\t%s%s" % (("$%f" % cost).ljust(15), resource_type)

            print "\t\t\t%s      %s" % ("-" * 9, "-" * 40)
            print "\t\t\t$%-14f" % sum(resource_type_to_pricing.values())

            print "\t%s"%("-"*100)
            print "\tWorkflow Cost: $%f (cpu: $%f | disk: $%f | other: $%f)\n" % (total, cpu_cost, pd_cost, other_cost)
        
    # workspace_request = firecloud_api.get_workspace(ws_namespace, ws_name)
#     
#     if workspace_request.status_code != 200:
#         fail("Unable to find workspace: %s/%s  at  %s --- %s" % (ws_namespace, ws_name, workspace_request.text))
#         
#     workspace = workspace_request.json()
#     bucketName = workspace["workspace"]["bucketName"]
#     
#     print "Retrieving submission..."
#     sub_json = firecloud_api.get_submission(ws_namespace, ws_name, submission_id).json()
#     
#     #mc_ns = sub_json["methodConfigurationNamespace"]
#     #mc_name = sub_json["methodConfigurationName"]
#     
#     
#     workflow_outputs = []
#     longest_name = 0
#     workflows = sub_json["workflows"]
#     
#     num_workflows = len(workflows)
#     workflow_idx = 1
#     for wf in workflows:
#         workflow_id = wf["workflowId"]
#         print "\tProcessing workflow: %d/%d --- %s" % (workflow_idx, num_workflows, workflow_id)
#         workflow_idx += 1
#         
#         workflow_meta = get_workflow_metadata(ws_namespace, ws_name, submission_id, workflow_id)
#                 
#         outputs = workflow_meta["outputs"]
#         #longest = len(max(outputs.keys(), key=len))
#         #longest_name = max(longest_name, longest)
#         
#         for output_name in outputs.keys():
#             output = outputs[output_name]
#             if re.match("gs://.*", output):
#                 workflow_outputs.append((output_name, output))
#     
#     print "Files to keep from workflow outputs:"
#     for output in workflow_outputs:
#         #print "\t%s: %s" % (output[0].ljust(longest_name), output[1])
#         print "\t%s:\n\t\t%s\n" % (output[0].ljust(longest_name), output[1])
#     
#     
#     
#     
#     
#     files_to_delete = []
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
    parser = DefaultArgsParser(description="Print a report about costs for a given submission.")

    # Core application arguments
    parser.add_argument('-a', '--all', dest='all_submissions', action='store_true', required=False, help='If set, this will use the admin endpoint to gather all submissions in the system rather than only submissions you have access to.  Note: this overrides the -p flag.')
    parser.add_argument('-p', '--namespace', dest='ws_namespace', action='store', required=True, help='Workspace namespace')
    parser.add_argument('-n', '--name', dest='ws_name', action='store', required=True, help='Workspace name')

    # parser.add_argument('-q', '--query_table_project', dest='query_table_project', action='store', required=False, help='Name of the project that contains the big query export table')
    # parser.add_argument('-t', '--query_table_name', dest='query_table_name', action='store', required=True, help='Name of the big query table')
    #
    parser.add_argument('-s', '--subid', dest='submission_id', action='store', required=False, help='Optional Submission ID to limit the pricing breakdown to')
    parser.add_argument('-w', '--wfid', dest='workflow_id', default=[], action='append', required=False, help='Optional Workflow ID to limit the pricing breakdown to.  Note that this requires a submission id to be passed as well."')

    
    # Call the appropriate function for the given subcommand, passing in the parsed program arguments
    args = parser.parse_args()

    #query_table_project = args.query_table_project if args.query_table_project else args.ws_namespace

    #get_pricing(query_table_project, args.query_table_name, args.ws_namespace, args.ws_name)

    if args.workflow_id and not args.submission_id:
        fail("Submission ID must also be provided when querying for a Workflow ID")

    get_pricing(args.ws_namespace, args.ws_name, args.submission_id, args.workflow_id)


if __name__ == "__main__":
    main()
