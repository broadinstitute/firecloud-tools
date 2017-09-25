## Report on the estimate storage cost for workspaces
This script will display the estimated storage costs for a single workspace or all workspaces in a given project.  This is the same information as shown on the summary page of a workspace, but provides a way to check costs quickly.  Note that gathering pricing info requires editor access or above to the workspace.  

Usage:

* All workspaces in a project:   ./run.sh storage_costs_estimates/storage_costs_estimates.py -p \<firecloud billing project name\>
* A single workspace:            ./run.sh storage_costs_estimates/storage_costs_estimates.py -p \<firecloud billing project name\> -n \<workspace name\>