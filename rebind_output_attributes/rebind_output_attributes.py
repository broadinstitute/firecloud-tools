#!/usr/bin/env python
from common import *

def fix_outputs(ws_namespace, ws_name, submission_id, expressions_override):
    # get the workspace
    workspace_request = firecloud_api.get_workspace(ws_namespace, ws_name)
    
    if workspace_request.status_code != 200:
        fail("Unable to find workspace: %s/%s  at  %s --- %s" % (ws_namespace, ws_name, workspace_request.text))

    # get the submission
    submission_request = firecloud_api.get_submission(ws_namespace, ws_name, submission_id)
    if submission_request.status_code != 200:
        fail("Unable to find submission: %s" % submission_id)
    submission_json = submission_request.json()   
    
    # translate the submission entity into workflow entity type
    submission_entity_json = submission_json["submissionEntity"]
    submission_entity_type = submission_entity_json["entityType"]
    # either the submission entity type will be the same as the workflow entity type 
    # (when it's a submission on a single entity) or it will be a set of those entities
    # - if it's a set, just strip _set from the end to get the type the set contains
    workflow_entity_type = submission_entity_type.replace("_set", "")
    
    
    # create an update TSV with the attributes to bind back to the data model from this 
    # given submission's outputs
    tsv_filename = 'entity_update.tsv'
    with open(tsv_filename, 'wb') as entity_update_tsv:
        entity_writer = csv.writer(entity_update_tsv, delimiter='\t')

        # id column name is entityType_id
        entity_id_column = "%s_id" % workflow_entity_type
        # initial headers need to be update:entityType_id
        tsv_headers = ['update:%s' % entity_id_column]

        # get the method config used by this submission
        mc_namespace = submission_json["methodConfigurationNamespace"]
        mc_name = submission_json["methodConfigurationName"]
        method_config_json = firecloud_api.get_workspace_config(ws_namespace, ws_name, mc_namespace, mc_name).json()

        # go through each output and create a header for the attribute that is needed
        mc_outputs = method_config_json["outputs"]
        mc_output_keys = mc_outputs.keys()
        for mc_output_key in mc_output_keys:
            # if the user provided expression to override for this output then use that instead
            if expressions_override and mc_output_key in expressions_override:
                output_expression = expressions_override[mc_output_key]
            # otherwise get the output expression from the method config
            else:
                output_expression = mc_outputs[mc_output_key]
            output_attribute = output_expression.replace("this.", "")
            tsv_headers.append(output_attribute)

        for override_key in expressions_override:

            # if the user provided expression doesn't override an output, then make sure to bind that too
            if override_key not in mc_output_keys:
                mc_output_keys.append(override_key)
                output_expression = expressions_override[override_key]
                output_attribute = output_expression.replace("this.", "")
                tsv_headers.append(output_attribute)

        entity_writer.writerow(tsv_headers)

        # go through each workflow in this submission
        submission_workflows_json = submission_json["workflows"]
        succeeded_workflows = [w for w in submission_workflows_json if w["status"] == "Succeeded"]

        num_workflows = len(succeeded_workflows)
        for workflow_idx, workflow_json in enumerate(succeeded_workflows):
            workflow_id = workflow_json["workflowId"]

            print "Processing workflow %d of %d: %s" % (workflow_idx+1, num_workflows, workflow_id)
            entity_attribute_updates = []

            workflow_entity_name = workflow_json["workflowEntity"]["entityName"]
            # the first column needs to be the name of the entity
            entity_attribute_updates.append(workflow_entity_name)

            # get workflow metadata and outputs that were produced
            workflow_metadata_json = get_workflow_metadata(ws_namespace, ws_name, submission_id, workflow_id)
            workflow_outputs_json = workflow_metadata_json["outputs"]

            # go through each method config output in the same order as the headers
            for mc_output_key in mc_output_keys:
                workflow_output = workflow_outputs_json[mc_output_key]

                # add the value from this workflow output to the same column as the attribute to bind it to
                entity_attribute_updates.append(workflow_output)

            # write the row values for this entity
            entity_writer.writerow(entity_attribute_updates)

    upload = prompt_to_continue("Update TSV file has been produced.  Would you like to upload this file?")

    if upload:
        print "Uploading updated entities TSV..."
        upload_request = firecloud_api.upload_entities_tsv(ws_namespace, ws_name, tsv_filename)

        if upload_request.status_code != 200:
            print "Error uploading updated entities TSV:", upload_request.text

        print "Upload complete."
        os.remove(tsv_filename)
    else:
        print "The file can be reviewed and manually uploaded - see %s" % tsv_filename


def main():
    setup()

    # The main argument parser
    parser = ArgumentParser(description="Use an existing submission from a workspace to bind attributes back to the data model.  "
                                        "This can be used to fix issues with binding that may have occurred, or to revert outputs "
                                        "to a previous submission.  Additionally, an optional expression override can be used to "
                                        "provide a new output expreession for a given output (e.g. to bind an output attribute that "
                                        "was not originally bound.")
 
    parser.add_argument('-u', '--url', dest='fc_url', action='store', required=False, help='If set, this will override which api is used (default is https://api.firecloud.org/api)')

    # Core application arguments
    parser.add_argument('-p', '--workspace_namespace', dest='ws_namespace', action='store', required=True, help='Workspace namespace')
    parser.add_argument('-n', '--workspace_name', dest='ws_name', action='store', required=True, help='Workspace name')
    parser.add_argument('-s', '--submission_id', dest='submission_id', action='store', required=True, help='Submission Id')
    parser.add_argument('-e', '--expressions_override', dest='expressions_override', action='store', required=False, help='Optional argument to override output expressions used in the method config for this submission.  Syntax is in the form \'{"output_name": "expression"}\'')

    
    # Call the appropriate function for the given subcommand, passing in the parsed program arguments
    args = parser.parse_args()
    
    if args.fc_url:
        firecloud_api.PROD_API_ROOT = args.fc_url
     
    print "Note -- this script has the following limitations:"
    print "        *  The output expressions must all be of the form 'this.attribute_name' - this does not handle " \
          "           cases such as 'this.case_sample.attribute_name'"
    print "        *  The root entity type chosen has to be either a single entity of root entity type or a set of " \
          "           those entities.  This will not work for instance if your method runs on a sample and you " \
          "           chose a pair with the launch expression 'this.case_sample'."
    print "        *  The method config used for this submission can not have been deleted."

    continue_script = prompt_to_continue("continue?")
    
    if continue_script:
        fix_outputs(args.ws_namespace, args.ws_name, args.submission_id, json.loads(args.expressions_override))
    else:
        print "Exiting."

if __name__ == "__main__":
    main()
