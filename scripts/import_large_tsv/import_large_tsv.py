##!/usr/bin/env python
from common import *
import tempfile




def main():
    setup()

    # The main argument parser
    parser = DefaultArgsParser(description="Imports a TSV file when it is too large to import from FireCloud.")

    # Core application arguments
    parser.add_argument('-p', '--namespace', dest='ws_namespace', action='store', required=True, help='Workspace namespace')
    parser.add_argument('-n', '--name', dest='ws_name', action='store', required=True, help='Workspace name')
    parser.add_argument('-f', '--tsv_file', dest='tsv_file', action='store', required=True, help='TSV file to import.')

    args = parser.parse_args()

    tsv_strings = []
    with open(args.tsv_file, "r") as tsvfile:
        headers = tsvfile.readline()

        tsv_data = headers
        for i, line in enumerate(tsvfile):
            tsv_data += line

            if i > 0 and i % 200 == 0:
                tsv_strings.append(tsv_data)
                tsv_data = headers

        # catch the last lines from the tsv file that aren't caught by the % above
        tsv_strings.append(tsv_data)

        pb = ProgressBar(0, len(tsv_strings), "Split TSV files uploaded")
        pb.print_bar()
        for tsv_string in tsv_strings:
            request = firecloud_api.upload_entities(args.ws_namespace, args.ws_name, tsv_string)
            if request.status_code != 200:
                fail(request.text)
            pb.increment()
            pb.print_bar()

if __name__ == "__main__":
    main()