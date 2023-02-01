
## Register a service account for use in FireCloud
This script will register a service account so that it can be used in FireCloud.  This means that the service account can be used to call any of the FireCloud APIs, and that a given workspace/method can be shared with the service account.  Note that a service account CANNOT be used to launch a method configuration at this time.

In order to run this script you will need to download the credentials JSON file for your service account.  See https://cloud.google.com/storage/docs/authentication#generating-a-private-key for information on creating a service account credentials JSON file.

Usage (from the main directory where run.sh resides):

```./run.sh scripts/register_service_account/register_service_account.py -j <path to your service account credentials json file> -e <email address for owner of this service account, it's where notifications will go>```

Usage (using Docker):

```docker run --rm -it -v "$HOME"/.config:/.config -v <path to your service account credentials json file>:/svc.json broadinstitute/firecloud-tools python /scripts/register_service_account/register_service_account.py -j /svc.json -e <email address for owner of this service account, it's where notifications will go>```

## Register a service account for use in FireCloud without a JSON credentials keyfile
Some organizations don't allow downloads of json keyfiles. In this case, it is still possible to run a VM with the credentials of a service account, and then use those credentials to make the request. You will need to run the modified version of the script in order to do this.

1. Create a VM with the service account's identity
    1. Navigate to the Create new Instance page in GCP for the project containing the service account
    2. Choose anything for name, size, and region. 
    3. Most importantly **be sure that the service account** is selected in the Identity and API access section
    4. We also need to enable the email, profile, and openid scopes to the service account. Unfortunately the UI doesn't allow us to add scopes for these kinds of service accounts. However if we create the VM through a gcloud command, we can add the scopes. **Do not click create**, instead click on "Equivalent Command Line" and copy the command.    
    5.  Edit the command to add the email, profile and openid scope. Run this command. For example, my command looks like:
```
gcloud compute instances create instance-1 \
  --project=terra-foo-foo-dev \
  --zone=us-central1-a \
  --machine-type=e2-medium \
  --network-interface=network-tier=PREMIUM,subnet=default \
  --maintenance-policy=MIGRATE \
  --provisioning-model=STANDARD \
  --service-account=foo-example-sa@terra-foo-foo-dev.iam.gserviceaccount.com \
  --scopes=https://www.googleapis.com/auth/cloud-platform,email,profile,openid \
  --create-disk=auto-delete=yes,boot=yes,device-name=instance-1,image=projects/debian-cloud/global/images/debian-11-bullseye-v20220920,mode=rw,size=10,type=projects/terra-foo-foo-dev/zones/us-central1-a/diskTypes/pd-balanced \
  --no-shielded-secure-boot \
  --shielded-vtpm \
  --shielded-integrity-monitoring \
  --reservation-affinity=any
```
2. SSH to the VM and run the Python script
Once the above VM is created and running, you can SSH to it. API requests made while in this VM will be made with the service account's credentials. 
    1. SSH to the VM
    2. The Python script requires a few libraries to be installed via pip. Pip isn't installed by default, so install pip via `sudo apt-get install python3-pip`
    3. Install google-auth, urllib3 and firecloud via `pip3 install google-auth urllib3 firecloud`
    4. Run the `register_service_account_no_keyfile.py` script
    ```
    python3 register_service_account_no_keyfile.py --owner_email=my_email@domain.com --first_name=Foo --last_name=Sa
    ```
    5.  If it worked, the script should output something like
```The service account foo-example-sa@terra-foo-foo-dev.iam.gserviceaccount.com is now registered with FireCloud. You can share workspaces with this address, or use it to call APIs.```
