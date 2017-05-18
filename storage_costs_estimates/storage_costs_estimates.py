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
        for ws in firecloud_api.list_workspaces().json():
            if ws["workspace"]["namespace"] == args.ws_namespace:
                workspace_name = ws["workspace"]["name"]
                estimate = get_workspace_storage_estimate(args.ws_namespace, workspace_name)
                if estimate.status_code == 200:
                    print "%s: %s" % (workspace_name, get_workspace_storage_estimate(args.ws_namespace, workspace_name).json()["estimate"])


if __name__ == "__main__":
    main()
