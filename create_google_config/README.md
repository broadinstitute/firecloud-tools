## Prerequisites
* Python 2.7
* Set the Application Default Credentials 
	* To do this, run `gcloud auth application-default login`
* [Google Billing Project](https://cloud.google.com/billing/docs/how-to/manage-billing-account#create_a_new_billing_account) to pay for storage and compute

## Install prerequisites

```
$ python setup.py install
```

## Set up Cromwell configuration for running on Google Cloud

```
$ python create_google_config.py
```