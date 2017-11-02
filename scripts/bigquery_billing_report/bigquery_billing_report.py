##!/usr/bin/env python
from common import *


def print_submission_pricing(ws_namespace, ws_name, query_sub_id, query_workflow_id, show_all_calls,
                             bill_query_to_project_name, dataset_project_name, dataset_name, print_queries):
    print "Retrieving submissions in workspace..."
    workspace_request = firecloud_api.get_workspace(ws_namespace, ws_name)

    if workspace_request.status_code != 200:
        fail("Unable to find workspace: %s/%s --- %s" % (ws_namespace, ws_name, workspace_request.text))

    submissions_json = firecloud_api.list_submissions(ws_namespace, ws_name).json()

    workflow_dict = {}
    submission_dict = {}
    for submission_json in submissions_json:
        sub_id = submission_json["submissionId"]

        if query_sub_id and sub_id not in query_sub_id:
            continue;

        sub_details_json = firecloud_api.get_submission(ws_namespace, ws_name, sub_id).json()
        submission_dict[sub_id] = sub_details_json

        for wf in (wf for wf in sub_details_json["workflows"] if
                   "workflowId" in wf and "workflows" in sub_details_json):
            wf_id = wf["workflowId"]
            if query_workflow_id and wf_id not in query_workflow_id:
                continue;

            workflow_dict[wf_id] = {"submission_id": sub_id, "workflow": wf}

    firecloud_api._fiss_access_headers = _fiss_access_headers_local
    if len(workflow_dict) == 0:
        fail("No submissions or workflows matching the criteria were found.")

    # Imports the Google Cloud client library
    from google.cloud import bigquery

    subquery_template = "labels_value LIKE \"%%%s%%\""
    subquery_list = [subquery_template % workflow_id for workflow_id in workflow_dict]

    print "Gathering pricing data..."

    client = bigquery.Client(bill_query_to_project_name)
    dataset = client.dataset(dataset_name)

    matched_workflow_ids = set()

    workflow_id_to_cost = defaultdict(list)
    num_workflow_ids_per_query = 1000
    total_row_count = 0
    for i in xrange(0, len(subquery_list), num_workflow_ids_per_query):
        query_index_start = i
        query_index_end = i + num_workflow_ids_per_query
        subquery_subset = subquery_list[query_index_start:query_index_end]
        workflows_subquery = " OR ".join(subquery_subset)

        query = """
                             SELECT GROUP_CONCAT(labels.key) WITHIN RECORD AS labels_key,
                                    GROUP_CONCAT(labels.value) WITHIN RECORD labels_value,
                                    cost,
                                    product,
                                    resource_type
                             FROM [%s:%s]
                             WHERE project.id = '%s'
                                     AND
                                   labels.key IN ("cromwell-workflow-id",
                                                  "cromwell-workflow-name",
                                                  "cromwell-sub-workflow-name",
                                                  "wdl-task-name",
                                                  "wdl-call-alias")
                             HAVING %s
                             # uncomment for quick testing:
                             #LIMIT 1
                           """ % (dataset_project_name, dataset_name, ws_namespace, workflows_subquery)
        if print_queries:
            print query

        query_results = client.run_sync_query(query)

        # Use standard SQL syntax for queries.
        # See: https://cloud.google.com/bigquery/sql-reference/
        # query_results.use_legacy_sql = False

        query_results.run()

        print "Retrieving BigQuery cost information for workflows %d to %d of %d..." % (
            query_index_start, min(query_index_end, len(workflow_dict)), len(workflow_dict))

        page_token = None

        while True:
            rows = query_results.fetch_data(
                max_results=1000,
                page_token=page_token)
            for row in rows:
                total_row_count += 1

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
                workflow_id_to_cost[workflow_id].append(
                    CostRow(cost, product, resource_type, workflow_id, task_name, call_name))
                if workflow_id in workflow_dict:
                    matched_workflow_ids.add(workflow_id)

            if not page_token:
                break

    submission_id_to_workflows = defaultdict(list)
    for wf_id in workflow_dict:
        workflow = workflow_dict[wf_id]
        submission_id_to_workflows[workflow["submission_id"]].append(workflow)

    wf_count = 1
    wf_total_count = len(workflow_dict)
    for submission_id, workflows in submission_id_to_workflows.iteritems():
        if len(workflows) == 0:
            print "No Workflows."
            continue;

        submitter = submission_dict[submission_id]["submitter"]

        print ".--- Submission: %s (submitted by %s)" % (submission_id, submitter)
        submission_total = 0.0
        submission_pd_cost = 0.0
        submission_cpu_cost = 0.0
        submission_other_cost = 0.0

        workflow_ids_with_no_cost = set()
        for workflow in workflows:
            workflow_json = workflow["workflow"]
            wf_id = workflow_json["workflowId"]

            if wf_id not in workflow_id_to_cost:
                workflow_ids_with_no_cost.add(wf_id)
                continue

            workflow_metadata_json = get_workflow_metadata(ws_namespace, ws_name, submission_id, wf_id)

            if "calls" not in workflow_metadata_json:
                # print ws_namespace, ws_name, submission_id, wf_id, workflow_metadata_json
                continue

            # workflow_metadata_json = workflow_id_to_metadata_json[wf_id]
            calls_lower_json = dict(
                (k.split(".")[-1].lower(), v) for k, v in workflow_metadata_json["calls"].iteritems())

            if len(calls_lower_json) == 0:
                print "\tNo Calls."
            else:
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

            print "|\t.--- Workflow %d of %d: %s (%s)" % (wf_count, wf_total_count, wf_id, workflow_json["status"])
            wf_count += 1

            call_name_to_cost = defaultdict(list)
            for c in workflow_id_to_cost[wf_id]:
                call_name_to_cost[c.call_name].append(c)

            total = 0.0
            pd_cost = 0.0
            cpu_cost = 0.0
            other_cost = 0.0
            for call_name in call_name_to_cost:
                calls = calls_lower_json[call_name] if call_name in calls_lower_json else calls_lower_translated_json[
                    call_name]

                call_pricing = call_name_to_cost[call_name]

                resource_type_to_pricing = defaultdict(int)
                for pricing in call_pricing:
                    if pricing.cost > 0:
                        resource_type_to_pricing[pricing.resource_type] += pricing.cost

                if call_name in calls_lower_json:
                    num_calls = len(calls)
                else:
                    if call_name in calls_lower_translated_json:
                        num_calls = len(calls_lower_translated_json[call_name])
                    else:
                        num_calls = 0
                print "|\t|\t%-10s%s:" % ("(%dx)" % num_calls, call_name)

                if show_all_calls:
                    for call in sorted(calls, key=lambda call: call["attempt"]):
                        start = dateutil.parser.parse(call["start"])
                        end = dateutil.parser.parse(call["end"])
                        preempted = "[preempted]" if call["preemptible"] == True and call[
                                                                                         "executionStatus"] == "RetryableFailure" else ""
                        print "|\t|\t             * Attempt #%d: start: %s - end: %s (elapsed: %s) %s" % (
                            call["attempt"], start, end, end - start, preempted)

                sorted_costs = sorted(resource_type_to_pricing, key=lambda rt: resource_type_to_pricing[rt],
                                      reverse=True)

                if len(sorted_costs) > 0:
                    for resource_type in sorted_costs:
                        cost = resource_type_to_pricing[resource_type]

                        if cost == 0:
                            continue

                        total += cost
                        if "pd" in resource_type.lower():
                            pd_cost += cost
                        elif "cpu" in resource_type.lower():
                            cpu_cost += cost
                        else:
                            other_cost += cost

                        print "|\t|\t\t\t%s%s" % (("$%f" % cost).ljust(15), resource_type)
                else:
                    workflow_ids_with_no_cost.add(wf_id)
                    print "|\t|\t\t\t(missing cost information)"

            print "|\t|    %s" % ("-" * 100)
            print "|\t'--> Workflow Cost: $%f (cpu: $%f | disk: $%f | other: $%f)\n|" % (
                total, cpu_cost, pd_cost, other_cost)

            submission_total += total
            submission_pd_cost += pd_cost
            submission_cpu_cost += cpu_cost
            submission_other_cost += other_cost

        # only a single workflow
        if query_workflow_id != None and len(query_workflow_id) > 0:
            print "'%s" % ("-" * 100)
        else:
            print "|    %s" % ("-" * 100)
            missing_workflows = len(workflow_ids_with_no_cost) > 0
            caveat_text = (" (** for %d out of %d workflows)") % (
                wf_count - len(workflow_ids_with_no_cost), wf_count) if missing_workflows else ""
            print "'--> Submission Cost%s: $%f (cpu: $%f | disk: $%f | other: $%f)\n" % (
                caveat_text, submission_total, submission_cpu_cost, submission_pd_cost, submission_other_cost)
            if missing_workflows:
                print "     ** %d workflows without cost information, e.g. %s" % (
                    len(workflow_ids_with_no_cost), next(iter(workflow_ids_with_no_cost)))


class CostRow():
    def __init__(self, cost, product, resource_type, workflow_id, task_name, call_name):
        self.cost = cost
        self.product = product
        self.resource_type = resource_type
        self.workflow_id = workflow_id
        self.task_name = task_name
        self.call_name = call_name


last_token = None
token_request_count = 0


def get_token():
    global token_request_count
    global last_token

    if token_request_count % 100 == 0:
        command = "gcloud auth print-access-token"
        last_token = subprocess.check_output(command, shell=True).decode().strip()

    token_request_count += 1

    return last_token


# this was added due to an issue with token expiring while running on a large submission.
# Not sure why the standard Google library was not handling that properly.
# TODO: generalize this or figure out why the Google library was not working as expected
def _fiss_access_headers_local(headers=None):
    """ Return request headers for fiss.
        Retrieves an access token with the user's google crededentials, and
        inserts FISS as the User-Agent.
    Args:
        headers (dict): Include additional headers as key-value pairs
    """
    credentials = GoogleCredentials.get_application_default()
    access_token = get_token()
    fiss_headers = {"Authorization": "bearer " + access_token}
    fiss_headers["User-Agent"] = firecloud_api.FISS_USER_AGENT
    if headers:
        fiss_headers.update(headers)
    return fiss_headers


def main():
    setup()

    # The main argument parser
    parser = DefaultArgsParser(description="Print a report about costs for a given submission.")

    # Core application arguments
    parser.add_argument('-p', '--namespace', dest='ws_namespace', action='store', required=True,
                        help='Workspace namespace')
    parser.add_argument('-n', '--name', dest='ws_name', action='store', required=True, help='Workspace name')

    parser.add_argument('-s', '--subid', dest='submission_id', action='store', required=False,
                        help='Optional Submission ID to limit the pricing breakdown to.  If not provided pricing for all submissions in this workspace will be reported.')
    parser.add_argument('-w', '--wfid', dest='workflow_id', default=[], action='append', required=False,
                        help='Optional Workflow ID to limit the pricing breakdown to.  Note that this requires a submission id to be passed as well."')

    parser.add_argument('-c', '--calls', dest='show_all_calls', action='store_true', required=False,
                        help='Expand information about each call.')

    parser.add_argument('-bp', '--bill_to_project', dest='bill_to_project', action='store', required=False,
                        help='Optional project to bill this Big Query query to.  This defaults to the workspace namespace if not provided.')
    parser.add_argument('-dp', '--dataset_project', dest='dataset_project', action='store', required=False,
                        help='Optional project where dataset is stored - defaults to the workspace project.  This defaults to the workspace namespace if not provided.')
    parser.add_argument('-dn', '--dataset_name', dest='dataset_name', action='store', required=False,
                        help='Optional dataset name where billing data is stored - defaults to billing_export.')

    parser.add_argument('-pq', '--print_queries', dest='print_queries', action='store_true', required=False,
                        help='Optional flag to cause all the BigQuery queries to be printed.')

    args = parser.parse_args()

    if args.workflow_id and not args.submission_id:
        fail("Submission ID must also be provided when querying for a Workflow ID")

    if not args.dataset_name:
        print "Note that this script expects the billing export table to be named 'billing_export'.  "

    # dataset project comes from argument if provided, otherwise assume the big query dataset is in the workspace project
    dataset_project = args.dataset_project if args.dataset_project else args.ws_namespace
    # bill to the provided argument if provided, otherwise bill to the dataset_project
    bill_query_to_project = args.bill_to_project if args.bill_to_project else dataset_project

    print_submission_pricing(args.ws_namespace, args.ws_name, args.submission_id, args.workflow_id, args.show_all_calls,
                             bill_query_to_project,
                             dataset_project,
                             args.dataset_name if args.dataset_name else 'billing_export',
                             args.print_queries)


if __name__ == "__main__":
    main()
