## Convenience wrapper to get a WDL into a workspace in FireCloud
This script will take a WDL and inputs json you have locally and push it to the methods repo, either create a new method config for you in your workspace (if it does not exist) or update an existing method config to point to your new method snapshot.  Optionally you can also launch the method config against a given entity.  This allows you to quickly iterate on a WDL that you want to run within FireCloud.

Usage:

* `./run.sh scripts/submit_workflow_source_files/submit_workflow_source_files.py -wns \<workspace project\> -wn \<workspace name\> -cns \<method config namespace\> -cn \<method config name\> -e \<root entity type\> -mns \<method namespace\> -mn \<method name\> -w \<local path to the wdl\> -i \<local path to the inputs file\> -l \<optional entity to launch against (of the same entity type as the root entity type)\>`
