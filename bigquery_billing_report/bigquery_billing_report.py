##!/usr/bin/env python
from common import *


def get_pricing(ws_namespace, ws_name, query_sub_id = None, query_workflow_id = None):
    print "Retrieving submissions in workspace..."       
    workspace_request = firecloud_api.get_workspace(ws_namespace, ws_name)
    
    if workspace_request.status_code != 200:
        fail("Unable to find workspace: %s/%s  at  %s --- %s" % (ws_namespace, ws_name, workspace_request.text))
        
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

    get_workflow_pricing(ws_namespace, ws_name, workflow_dict, query_workflow_id!=None and len(query_workflow_id) > 0)


class CostRow():
    def __init__(self, cost, product, resource_type, workflow_id, task_name, call_name):
        self.cost = cost
        self.product = product 
        self.resource_type = resource_type
        self.workflow_id = workflow_id
        self.task_name = task_name
        self.call_name = call_name

                      
def get_workflow_pricing(ws_namespace, ws_name, workflow_dict, singleWorkflowMode):
    if len(workflow_dict) == 0:
        fail("No submissions or workflows matching the criteria were found.")

    # Imports the Google Cloud client library
    from google.cloud import bigquery
    
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

        query_results = client.run_sync_query(query)

        # Use standard SQL syntax for queries.
        # See: https://cloud.google.com/bigquery/sql-reference/
        #query_results.use_legacy_sql = False

        query_results.run()

        print "Processing data..."

        page_token = None

        workflow_id_to_cost = defaultdict(list)
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
                workflow_id_to_cost[workflow_id].append(CostRow(cost, product, resource_type, workflow_id, task_name, call_name) )

            if not page_token:
                break

        submission_id_to_workflows = defaultdict(list)
        for wf_id in workflow_id_to_cost:
            workflow = workflow_dict[wf_id]
            submission_id_to_workflows[workflow["submission_id"]].append(workflow)

        for submission_id, workflows in submission_id_to_workflows.iteritems():
            print ".--- Submission:", submission_id
            submission_total = 0.0
            submission_pd_cost = 0.0
            submission_cpu_cost = 0.0
            submission_other_cost = 0.0

            for workflow in workflows:
                workflow_json = workflow["workflow"]
                wf_id = workflow_json["workflowId"]

                workflow_metadata_json = get_workflow_metadata(ws_namespace, ws_name, submission_id, wf_id)
                calls_lower_json = dict((k.split(".")[-1].lower(), v) for k, v in workflow_metadata_json["calls"].iteritems())
                calls_lower_translated_json = {}
                for calls_name, call_json in calls_lower_json.iteritems():
                    # from the Cromwell documentation call names can be translated into a different format
                    # under certain circumstances.  We will try translating it here and see if we get a match.
                    # Rules:
                    # Any capital letters are lowercased.
                    # Any character which is not one of [a-z], [0-9] or - will be replaced with -.
                    # If the start character does not match [a-z] then prefix with x--
                    # If the final character does not match [a-z0-9] then suffix with --x
                    # If the string is too long, only take the first 30 and last 30 characters and add --- between them.
                    # TODO: this is not a complete implementation - however I requested that cromwell metadata includes label
                    # TODO: so that this translation is not necessary
                    cromwell_translated_callname = re.sub("[^a-z0-9\-]", "-", call_name.lower())
                    calls_lower_translated_json[cromwell_translated_callname] = call_json

                print "|\t.--- Workflow: %s (%s)" % (wf_id, workflow_json["status"])

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

                    if call_name in calls_lower_json:
                        num_calls = len(calls_lower_json[call_name])
                    else:
                        if call_name in calls_lower_translated_json:
                            num_calls = len(calls_lower_translated_json[call_name])
                        else:
                            num_calls = 0

                    print "|\t|\t%-10s%s:" % ("(%dx)" % num_calls, call_name)

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

                            print "|\t|\t\t\t%s%s" % (("$%f" % cost).ljust(15), resource_type)
                print "|\t|    %s"%("-"*100)
                print "|\t'--> Workflow Cost: $%f (cpu: $%f | disk: $%f | other: $%f)\n|" % (total, cpu_cost, pd_cost, other_cost)

                submission_total += total
                submission_pd_cost += pd_cost
                submission_cpu_cost += cpu_cost
                submission_other_cost += other_cost

            if singleWorkflowMode:
                print "'%s" % ("-" * 100)
            else:
                print "|    %s" % ("-" * 100)
                print "'--> Submission Cost: $%f (cpu: $%f | disk: $%f | other: $%f)\n" % (submission_total, submission_cpu_cost, submission_pd_cost, submission_other_cost)


def main():
    setup()

    # The main argument parser
    parser = DefaultArgsParser(description="Print a report about costs for a given submission.")

    # Core application arguments
    parser.add_argument('-p', '--namespace', dest='ws_namespace', action='store', required=True, help='Workspace namespace')
    parser.add_argument('-n', '--name', dest='ws_name', action='store', required=True, help='Workspace name')

    parser.add_argument('-s', '--subid', dest='submission_id', action='store', required=False, help='Optional Submission ID to limit the pricing breakdown to.  If not provided pricing for all submissions in this workspace will be reported.')
    parser.add_argument('-w', '--wfid', dest='workflow_id', default=[], action='append', required=False, help='Optional Workflow ID to limit the pricing breakdown to.  Note that this requires a submission id to be passed as well."')

    args = parser.parse_args()

    if args.workflow_id and not args.submission_id:
        fail("Submission ID must also be provided when querying for a Workflow ID")

    print "Note that this script expects the billing export table to be named 'billing_export'.  "
    get_pricing(args.ws_namespace, args.ws_name, args.submission_id, args.workflow_id)


if __name__ == "__main__":
    main()
