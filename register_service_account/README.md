## Register a service account for use in FireCloud
This script will register a service account so that it can be used in FireCloud.  This means that the service account can be used to call any of the FireCloud APIs, and that a given workspace/method can be shared with the service account.  Note that a service account CANNOT be used to launch a method configuration at this time.

In order to run this script you will need to download the credentials JSON file for your service account.  See https://cloud.google.com/storage/docs/authentication#generating-a-private-key for information on creating a service account credentials JSON file.

Usage:

```/run.sh register_service_account/register_service_account.py -j <path to your service account credentials json file> -e <email address for owner of this service account>```
