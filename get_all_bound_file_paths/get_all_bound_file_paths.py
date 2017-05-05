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

    request = firecloud_api.list_entity_types(args.ws_namespace, args.ws_name)
    entity_types_json = request.json()

    print "entity_type\tentity_name\tattribute_name\tattribute_value"
    for entity_type in entity_types_json:
        entity_count = entity_types_json[entity_type]["count"]

        page_size = 1000
        num_pages = int(math.ceil(float(entity_count) / page_size))
        for i in range(1, num_pages + 1):
            for entity_json in get_entity_by_page(args.ws_namespace, args.ws_name, entity_type, i, page_size)["results"]:
                for attribute_name, attribute_value in entity_json["attributes"].iteritems():
                    if re.match(r"gs://", str(attribute_value)):
                        print "{0}\t{1}\t{2}\t{3}".format(entity_json["entityType"], entity_json["name"],
                                                          attribute_name, attribute_value)


if __name__ == "__main__":
    main()