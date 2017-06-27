##!/usr/bin/env python
from common import *


def main():
    setup()

    # The main argument parser
    parser = DefaultArgsParser(description="Export a TSV file for a given entity when that entity is too large to download from FireCloud.")

    # Core application arguments
    parser.add_argument('-p', '--namespace', dest='ws_namespace', action='store', required=True, help='Workspace namespace')
    parser.add_argument('-n', '--name', dest='ws_name', action='store', required=True, help='Workspace name')
    parser.add_argument('-e', '--entity_type', dest='entity_type', action='store', required=True, help='Type of the entity to get TSV data for.')
    parser.add_argument('-o', '--out_file', dest='output_file', action='store', required=True, help='Name of the tsv export file.')

    args = parser.parse_args()

    request = firecloud_api.list_entity_types(args.ws_namespace, args.ws_name)
    if request.status_code != 200:
        fail(request.text)

    entity_types_json = request.json()
    entity_count = entity_types_json[args.entity_type]["count"]
    print "%d %s(s) to gather..." % (entity_count,args.entity_type)
    attribute_names = entity_types_json[args.entity_type]["attributeNames"]

    with open(args.output_file, "w") as tsvfile:
        tsvfile.write("\t".join(attribute_names)+"\n")

        entity_data = []
        row_num = 0
        page_size = 1000
        num_pages = int(math.ceil(float(entity_count) / page_size))

        pool = mp.Pool(processes=2)
        entity_requests = []

        for i in range(1, num_pages + 1):
            entity_requests.append(pool.apply_async(get_entity_by_page,
                                                    args=(args.ws_namespace, args.ws_name, args.entity_type, i, page_size)))

        pb = ProgressBar(0, entity_count, "Entities gathered")

        for request in entity_requests:
            for entity_json in request.get(timeout=100)["results"]:
                attributes = entity_json["attributes"]
                values = []
                for attribute_name in attribute_names:
                    value = ""

                    if attribute_name in attributes:
                        value = attributes[attribute_name]
                    if attribute_name == "participant" or attribute_name == "sample":
                        value = value["entityName"]

                    values.append(value)

                tsvfile.write("\t".join(values)+"\n")
                row_num += 1
                pb.increment()
                pb.print_bar()
if __name__ == "__main__":
    main()