##!/usr/bin/env python
from common import *


def main():
    setup()

    # The main argument parser
    parser = DefaultArgsParser(description="Get inputs json from a given workflow submission.")

    # Core application arguments
    parser.add_argument('-p', '--namespace', dest='ws_namespace', action='store', required=True, help='Workspace namespace')
    parser.add_argument('-n', '--name', dest='ws_name', action='store', required=True, help='Workspace name')

    parser.add_argument('-s', '--subid', dest='submission_id', action='store', required=True, help='Submission ID for this workflow.')
    parser.add_argument('-w', '--wfid', dest='workflow_id', action='store', required=False, help='Workflow ID get the inputs json from."')

    args = parser.parse_args()

    print firecloud_api.get_workflow_metadata(args.ws_namespace, args.ws_name, args.submission_id, args.workflow_id).json()['submittedFiles']['inputs']

    
if __name__ == "__main__":
    main()