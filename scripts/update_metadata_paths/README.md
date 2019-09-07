## Change paths within the metadata of a workspace to point to a new bucket
This script is intended to deal with the problem of updating metadata in a workspace to point to new paths 
after you copy data (actual bucket files) from one workspace to another.  This can also be used if the 
underlying data that a workspace referred to got moved to a new bucket location.  The -r flag is used to specify
what old bucket names to replace with what new bucket names.  These take the form of `-r old_bucket_name=new_bucket_name`.
For example if we wanted to take old bucket paths like 
`gs://my_old_bucket_name/foo.txt` and replace those with `gs://my_new_bucket_name/foo.txt`
we would pass the following: `-r my_old_bucket_name=my_new_bucket_name`.  Multiple `-r` flags can be passed if we want
to change more than one path in metadata at once.  

NOTE: this does not currently support set types and only deals with regular entities (e.g. participant, sample, etc)

Usage (from the main directory where run.sh resides):

```./run.sh scripts/update_metadata_paths/update_metadata_paths.py -p <workspace project name> -n <workspace name> -r old_bucket_name=new_bucket_name -r other_old_bucket_name=other_new_bucket_name```

Usage (using Docker):

```docker run --rm -it -v "$HOME"/.config:/.config broadinstitute/firecloud-tools python /scripts/update_metadata_paths/update_metadata_paths.py -p <workspace project name> -n <workspace name> -r old_bucket_name=new_bucket_name -r other_old_bucket_name=other_new_bucket_name```

