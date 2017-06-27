##!/usr/bin/env python
from common import *


def main():
    setup()

    # The main argument parser
    parser = DefaultArgsParser(description="Print estimated storage costs for a given workspace or for all workspaces you have editor or above access to in a given project.")

    # Core application arguments
    parser.add_argument('-p', '--namespace', dest='ws_namespace', action='store', required=True, help='Workspace namespace')
    parser.add_argument('-n', '--name', dest='ws_name', action='store', required=False, help='Optional workspace name')

    args = parser.parse_args()

    if args.ws_name:
        print "%s: %s" % (args.ws_name, get_workspace_storage_estimate(args.ws_namespace, args.ws_name).json()["estimate"])
    else:
        workspaces_json = firecloud_api.list_workspaces().json()
        longest_name = len(max([ws["workspace"]["name"] for ws in workspaces_json], key=len))

        requests = {}
        pool = mp.Pool(processes=20)

        for ws in workspaces_json:
            if ws["workspace"]["namespace"] == args.ws_namespace:
                workspace_name = ws["workspace"]["name"]
                workflow_id = ws["workspace"]["workspaceId"]
                requests[workflow_id] = pool.apply_async(get_workspace_storage_estimate, (args.ws_namespace, workspace_name))

        print "Getting cost information..."

        pb = ProgressBar(0, len(requests), "Workspace bucket prices gathered")
        workflow_id_to_cost = {}
        for workflow_id, request in requests.iteritems():
            result = request.get(timeout=100)
            estimate_request = get_workspace_storage_estimate(args.ws_namespace, workspace_name)
            if result.status_code == 200 and estimate_request.status_code == 200:
                cost = estimate_request.json()["estimate"]
                workflow_id_to_cost[workflow_id] = cost
            pb.increment()
            pb.print_bar()

        pool.close()

        cost_info = []
        for ws in [ws_json for ws_json in workspaces_json if ws_json["workspace"]["workspaceId"] in workflow_id_to_cost]:
            workflow_id = ws["workspace"]["workspaceId"]

            workspace_name = ws["workspace"]["name"]

            cost_info.append( (longest_name, workspace_name + ":", 15, workflow_id_to_cost[workflow_id], ws["workspace"]["bucketName"]) )


        # sorted by cost
        for cost in sorted(cost_info, key=lambda cost: cost[3], reverse=True):
            print "%-*s%-*sgs://%s" % cost


if __name__ == "__main__":
    main()
