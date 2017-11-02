## Report on the cost of submissions/workflows in a given workspace
This script will provide a cost breakdown for either (a) all workflows in all submissions (b) a specific submission or (c) a specific workflow in a given workspace.

Usage:

* All submissions in a workspace: ./run.sh bigquery_billing_report/bigquery_billing_report.py -p \<firecloud billing project name\> -n \<workspace name\>
* All workflows in a submission:  ./run.sh bigquery_billing_report/bigquery_billing_report.py -p \<firecloud billing project name\> -n \<workspace name\> -s \<submission id\>
* A single workflow:            ./run.sh bigquery_billing_report/bigquery_billing_report.py -p \<firecloud billing project name\> -n \<workspace name\> -s \<submission id\> -w \<workflow id\>


* For Big Query datasets not exported to the firecloud billing project:
  * -dp <big query dataset project> - optional argument to provide a different project
  * -dn <big query dataset name>    - optional argument for name of dataset where big query exports go to
  * -bp <project to run big query query within> - optional argument for project to run the query within - needs to be a project you have ability to run BQ queries within
* Other arguments:
  * -pq - optionally print the BQ queries
  * -c - optionally print info about all calls

The output is in the following form per workflow:
```
|       .--- Workflow: \<workflow id\> (\<workflow status\>)
|       |       (\<num calls\>x)  \<call name\>:
|       |                       $0.040999      Preemptible Standard Intel N1 1 VCPU running in Americas
|       |                       $0.035065      Storage PD Capacity
|       |    ----------------------------------------------------------------------------------------------------
|       '--\> Workflow Cost: $0.076064 (cpu: $0.040999 | disk: $0.035065 | other: $0.000000)
```

_Note_: (\<num_calls\>x) refers to how many calls for this task there were - e.g. (1x) - for example if this was a preemptible task and it got preempted once this would show up as 2x since
  the first call was preempted and the second succeeded.  If this was a scatter task 10 ways wide this would show 10x.
