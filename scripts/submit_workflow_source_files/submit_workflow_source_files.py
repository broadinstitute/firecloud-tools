from common import *
from pprint import pprint

def escapedText(fileName):

    with open(fileName, 'r') as f:
        fileContent = f.read()
        escapedText = json.dumps(fileContent)

    return escapedText


def main():
    setup()

    # The main argument parser
    parser = DefaultArgsParser(description="Create Method and Method Config from WDL source file and inputs json")

    # Core application arguments
    parser.add_argument('-mns', '--method-namespace', dest='method_namespace', action='store', required=True, help='Method namespace')
    parser.add_argument('-mn', '--method-name', dest='method_name', action='store', required=True, help='Method name')

    parser.add_argument('-wns', '--workspace-namespace', dest='workspace_namespace', action='store', required=True, help='Workspace namespace')
    parser.add_argument('-wn', '--workspace-name', dest='workspace_name', action='store', required=True, help='Workspace name')

    parser.add_argument('-w', '--wdl-file', dest='wdl_file', action='store', required=True, help='WDL source (File)')
    parser.add_argument('-i', '--wdl-inputs', dest='wdl_inputs', action='store', required=False, help='WDL inputs (File)')

    #Optional arguments
    parser.add_argument('-s', '--method-synopsis', dest='method_synopsis', action='store', required=False, help='Method Synopsis (File)')
    parser.add_argument('-d', '--method-docs', dest='method_docs', action='store', required=False, help='Method Docs (File)')

    parser.add_argument('-cns', '--config-namespace', dest='config_namespace', action='store', required=False, help='Config namespace (Default: Method namespace')
    parser.add_argument('-cn', '--config-name', dest='config_name', action='store', required=False, help='Config name (Default: Method name)')

    parser.add_argument('-e', '--root-entity', dest='root_entity', action='store', required=False, help='Method config root entity (Default: participant)')

    parser.add_argument('-l', '--launch-entity', dest='launch_entity', action='store', required=False, help='Optional flag to launch method against a given entity')

    args = parser.parse_args()

    docs = args.method_docs if (args.method_docs) else None

    if args.method_synopsis is not None:
        with open (args.method_synopsis, 'r') as f:
            synopsis = f.read()
    else:
        synopsis = ""

    #TODO: Add error handling for failed method creation requests
    method_submission = firecloud_api.update_repository_method(args.method_namespace, args.method_name, synopsis, args.wdl_file, docs)

    if method_submission.status_code != 201:
        print method_submission
        fail("Unable to create method: %s %s" % (args.method_namespace, args.method_name))
    submission_json = method_submission.json()

    config_name = args.config_name if (args.config_name) else args.method_name
    config_namespace = args.config_namespace if (args.config_namespace) else args.method_namespace
    root_entity = args.root_entity if (args.root_entity) else "participant"
    #get inputs
    if args.wdl_inputs is not None:
        with open (args.wdl_inputs, 'r') as f:
            inputs_json = json.loads(f.read())
            #Filter out any key/values that contain #, and escape strings with quotes as MCs need this to not be treated as expressions
            inputs = {k:"\"{}\"".format(v) if isinstance(v, basestring) else v for k,v in inputs_json.iteritems() if '#' not in k}
    else:
        inputs = {}

    #Build JSON for method config
    method_body = {
        'name': config_name,
        'namespace': config_namespace,
        'methodRepoMethod': { 'methodNamespace': args.method_namespace,
                              'methodName': args.method_name,
                              'methodVersion': submission_json["snapshotId"]
                            },
        'rootEntityType': root_entity,
        "prerequisites": {},
        "methodConfigVersion": 1,
        "deleted": False,
        "inputs": inputs,
        "outputs": {} 
        }


    #print("method body: ")
    #print(method_body)

    #TODO: perform GET to see if the method config already exists and use update_workspace_config instead

    config_exists = firecloud_api.get_workspace_config(args.workspace_namespace, args.workspace_name, config_namespace, config_name)
    
    failed = False
    if config_exists.status_code == 200:
        config_submission = firecloud_api.update_workspace_config(args.workspace_namespace, args.workspace_name, config_namespace, config_name, method_body)
        if config_submission.status_code == 200:
            print "method config updated."
        else:
            failed = True
        
    else:
        config_submission = firecloud_api.create_workspace_config(args.workspace_namespace, args.workspace_name, method_body)
        update_status = config_submission.status_code
        if config_submission.status_code == 201:
            print "method config created."
        else:
            failed = True
        
    config_submission_json = config_submission.json()
    if failed:
        print("Failed request response:")
        print(config_submission_json)
        fail("Unable to create method config: %s %s " % (config_namespace, config_name))

    if args.launch_entity:
       launch_submission = firecloud_api.create_submission(args.workspace_namespace, args.workspace_name, 
                                                           config_namespace, config_name,
                                                           args.launch_entity, args.root_entity, "")
       if launch_submission.status_code == 201:
           submissionId = launch_submission.json()["submissionId"]
           print "Submission Launched - status can be checked at:"
           print "https://portal.firecloud.org/#workspaces/{}/{}/monitor/{}".format(args.workspace_namespace, args.workspace_name, submissionId)
       else:
           fail("Unable to launch submission - " + launch_submission.json())
if __name__ == "__main__":
    main()