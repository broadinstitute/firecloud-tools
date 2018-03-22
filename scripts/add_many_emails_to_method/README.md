## Update the permissions on a given method or config from a file with list of emails
This script takes in a file of email addresses (one per line) and uses those to set permissions on a given method or config.  All users in that file will get the same access level, so if other access levels are needed per "group" of users, you must use different files.

Run this as follows (from the main directory):
```./run.sh scripts/add_many_emails_to_method/add_many_emails_to_method.py -s <method/config namespace> -n <method/config name> -i <snapshot id> -t <type, either method or config> -r <access level, either READER or OWNER> -f <file with one email per line>```