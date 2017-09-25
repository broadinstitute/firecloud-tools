## Report on the cost of submissions/workflows in a given workspace
This script will provide a cost breakdown for either (a) all workflows in all submissions (b) a specific submission or (c) a specific workflow in a given workspace.

Usage:

* All submissions in a workspace: ./run.sh bigquery_billing_report/bigquery_billing_report.py -p \<firecloud billing project name\> -n \<workspace name\>
* All workflows in a submission:  ./run.sh bigquery_billing_report/bigquery_billing_report.py -p \<firecloud billing project name\> -n \<workspace name\> -s \<submission id\>
* A single submission:            ./run.sh bigquery_billing_report/bigquery_billing_report.py -p \<firecloud billing project name\> -n \<workspace name\> -s \<submission id\> -w \<workflow id\>


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
