## Get inputs json from an existing FireCloud workflow to use when calling Cromwell directly:
Run the script in the following manner (the output from this can be piped to a file, for instance):
```./run.sh scripts/get_workflow_inputs_json/get_workflow_inputs_json.py -p <workspace project> -n <workspace name> -s <submission id> -w <workflow id>```