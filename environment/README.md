## Set up Cromwell configuration for running on Google Cloud

To run this script:
```
./run.sh environment/setup.py
```

<!--To run using Docker:
```
docker run --rm -it -v "$HOME"/.config:/.config broadinstitute/firecloud-tools python /environment/setup.py
```-->

## Prerequisites
* Install the Google Cloud SDK from https://cloud.google.com/sdk/downloads
* Set the Application Default Credentials (run `gcloud auth application-default login`)
* Get a [Google Billing Project](https://cloud.google.com/billing/docs/how-to/manage-billing-account#create_a_new_billing_account)

When running without the run script or docker, check the packages that are pip
installed in either `run.sh` or the `Dockerfile`.