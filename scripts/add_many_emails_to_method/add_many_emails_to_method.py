##!/usr/bin/env python
from common import *


def main():
    setup()

    # The main argument parser
    parser = DefaultArgsParser(description="Use a file with a list of emails to add permissions to a given method/config in the repo.")

    # Core application arguments
    parser.add_argument('-s', '--namespace', dest='namespace', action='store', required=True, help='Method/config namespace')
    parser.add_argument('-n', '--name', dest='name', action='store', required=True, help='Method/config name')
    parser.add_argument('-i', '--snapshot_id', dest='snapshot_id', action='store', required=True, help='Method/config snapshot id')
    parser.add_argument('-t', '--type', dest='type', action='store', required=True, choices=['method', 'config'], help='Type of entity to modify: either method or config')
    parser.add_argument('-r', '--access_level', dest='access_level', action='store', choices=['OWNER', 'READER'], required=True, help='Access level to give each user in the file.')
    parser.add_argument('-f', '--input_file', dest='input_file', action='store', required=True, help='Input file containing one email address per line.')
	parser.add_argument('-fcu','--firecloud_user', dest='firecloud_user', action='store', required=True, help='Your firecloud user id.')
    args = parser.parse_args()

    permissions = []
    with open(args.input_file, "r") as emailFile:
        for e in emailFile:
            if e.replace("\n","")!=args.firecloud_user:
               permissions.append({"user":"%s"%e.replace("\n", ""), "role":"%s"%args.access_level})

        if args.type == 'method':
            existing = firecloud_api.get_repository_method_acl(args.namespace, args.name, args.snapshot_id).json()
            permissions.extend(existing)
            response = firecloud_api.update_repository_method_acl(args.namespace, args.name, args.snapshot_id, permissions)

            if response.status_code != 200:
                fail("Unable to update permissions on %s/%s/%s" % (args.namespace, args.name, args.snapshot_id))

            print "new permissions:", firecloud_api.get_repository_method_acl(args.namespace, args.name, args.snapshot_id).json()
        elif args.type == 'config':
            existing = firecloud_api.get_repository_config_acl(args.namespace, args.name, args.snapshot_id).json()
            permissions.extend(existing)
            response = firecloud_api.update_repository_config_acl(args.namespace, args.name, args.snapshot_id,
                                                                  permissions)

            if response.status_code != 200:
                fail("Unable to update permissions on %s/%s/%s" % (args.namespace, args.name, args.snapshot_id))

            print "new permissions:", firecloud_api.get_repository_config_acl(args.namespace, args.name,
                                                                              args.snapshot_id).json()
        else:
            fail("Type was not config or method.")
if __name__ == "__main__":
    main()