## Introduction

The purpose of this script is to create a configuration file for the [Cromwell execution engine](https://github.com/broadinstitute/cromwell) to run on [Google Cloud](https://cloud.google.com/). For more information see the [Cromwell documentation](http://cromwell.readthedocs.io/).

You can use this script to create a configuration file for an existing Google Project or it will prompt you for your Google Billing Account to create a new Google Project.

## Prerequisites
* Python 2.7
* [Google Billing Project](https://cloud.google.com/billing/docs/how-to/manage-billing-account#create_a_new_billing_account)
	* This is the billing account which will be charged for storage and compute when you store files in the Google Bucket and run pipelines on Google Compute Engine
* Set the Application Default Credentials 
	* To do this, run `$ gcloud auth application-default login`

## Pre-installation script

Run the following command to install the prerequisites for running the installation script:

```
$ python setup.py install
```

If you have both Python 2 and Pythong 3 installed, you may need to use `$ python2 setup.py install`.

## Run installation script

Run the following script to walk through the steps of setting up a configuration file for Cromwell.

```
$ python create_google_config.py
```