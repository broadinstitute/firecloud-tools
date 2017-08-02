## Rebind outputs from a given submission
This script will take a submission id from a given workspace and bind the outputs produced from this submission to the data model using the method config's output expressions.  

Optionally an expression override argument can be given that allows new output expressions to be defined and override the existing method config's output expressions.  This can be used for example to bind an output that did not originally have an expresion defined for it when the analysis was run.

Run this as follows (from the main directory):
```./run.sh rebind_output_attributes/rebind_output_attributes.py -p <workspace project> -n <workspace name> -s <submission id of the submission you want the outputs from> -e <optional, if used this can override output expressions used in the method config for this submission.  Syntax is in the form {"output_name": "expression"}```