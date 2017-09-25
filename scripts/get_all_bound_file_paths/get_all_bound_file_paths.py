##!/usr/bin/env python
from common import *


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

    all_bound = get_all_bound_file_paths(args.ws_namespace, args.ws_name)

    print "entity_type\tentity_name\tattribute_name\tattribute_value"
    for attribute_name, entity_json_list in all_bound.iteritems():
        for entity_json in entity_json_list:
            print "{0}\t{1}\t{2}\t{3}".format(entity_json["entityType"], entity_json["name"], attribute_name, entity_json["attributes"][attribute_name])


if __name__ == "__main__":
    main()