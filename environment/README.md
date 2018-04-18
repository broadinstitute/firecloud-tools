## Set up Cromwell configuration for running on Google Cloud

To run this script:
```
./run.sh environment/create_google_config.py
```

<!--To run using Docker:
```
docker run \-\-rm -it -v "$HOME"/.config:/.config broadinstitute/firecloud-tools python /environment/create_google_config.py
```-->

## Prerequisites
* Install the Google Cloud SDK from https://cloud.google.com/sdk/downloads
* Set the Application Default Credentials 
	* To do this, run `gcloud auth application-default login`
* [Google Billing Project](https://cloud.google.com/billing/docs/how-to/manage-billing-account#create_a_new_billing_account) to pay for storage and compute