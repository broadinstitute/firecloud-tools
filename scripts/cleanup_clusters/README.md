## List and delete Dataproc clusters across projects
This script will query Google projects for old Dataproc clusters and optionally call deletion APIs.

In order to run this script you will need to first run `gcloud auth login <you>@test.firecloud.org` as your admin user.

Usage (from the main directory):

```./cleanup_clusters/cleanup_clusters.sh [list|delete]```

`list` will print out the list of old clusters but not delete them.  `delete` will actually delete them.
