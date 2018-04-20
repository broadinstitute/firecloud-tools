##!/usr/bin/env python
import sys, os
from apiclient.discovery import build
# import GoogleCredentials
from oauth2client.client import GoogleCredentials
# import datetime
import datetime
import google.auth
from subprocess import check_output
from google.cloud import resource_manager
import json

# Google setup
credentials = GoogleCredentials.get_application_default()
# build a cloud billing API service
billing = build('cloudbilling', 'v1', credentials=credentials)
# build a cloud resource manager API service
crm = build('cloudresourcemanager', 'v1', credentials=credentials)


# The purpose of this script is to create a configuration file for Cromwell to run on Google Cloud with your local data.

def main():
	
	# Ensure that the user has not run this script before, to avoid overwriting an existing configuration file
	google_config_check()

	print "\nHello and welcome! This script is for users who want to run their WDL scripts\non Google Cloud using the Cromwell engine. This script will walk you through\nthe steps for setting up your configuration file, and then it will test your\nconfiguration with a short workflow.\n\nReady? Let's get started.\n"

	# Ensure that gcloud SDK is installed, which is necessary to run the rest of the script. 
	sdk_install_check()

	# Select Google project (new or existing)
	which_google_project()
	

# Ensure that the user has not run this script before, to avoid overwriting an existing configuration file
def google_config_check():
	
	# Check for .google_cromwell.config in ~/
	home = os.path.expanduser("~")
	existance = os.path.exists(home + '/.google_cromwell.config')
	
	# If there is a configuration file, exit
	if existance:
		print "\nYou already have a Cromwell configuration file. If you would like to clear this setup\nand create a new file, remove (or rename) the hidden file ~/.google_cromwell.config\n"
		sys.exit("Exiting.")

# Ensure that gcloud SDK is installed, which is necessary to run the rest of the script. 
def sdk_install_check():
		
	# If gcloud SDK is not installed, install it.
	if os.system('gcloud version') is not 0:
		
		# Ask if user wants to install the SDK
		installSdk = raw_input('\nYou do not have Google Cloud SDK installed, which you need to run this script.\nDo you want to install gcloud SDK? (yes or no)').lower()
		
		while not (installSdk.startswith("y") or installSdk.startswith("n")):
			installSdk = raw_input('\nPlease answer yes or no: ').lower()
			
		# User chooses to install
		if installSdk.startswith("y"):
			os.system('curl https://sdk.cloud.google.com | bash')
			shell = os.path.expanduser("$SHELL")
			# Need to create new shell to start using gcloud
			os.system('exec -l ' + shell)
			os.system('gcloud init')
			return
		
		# User chooses not to install SDK, exit the script because they can't continue.
		elif installSdk.startswith("n"):
			sys.exit("Exiting.")
	
	# Gcloud SDK is already installed, and user can continue with setup
	else:
		print "\nYou have Google Cloud SDK installed."
		return

# Which Google Project to use (new or existing)
def which_google_project():

	existing_project = raw_input('\nDo you have an existing Google project where you want to run workflows? (yes or no) ').lower()

	while not (existing_project.startswith("y") or existing_project.startswith("n")):	
		existing_project = raw_input('\nPlease answer yes or no: ').lower()
		
	# User has existing project
	if existing_project.startswith("y"):
		name = raw_input('\nEnter your Google project name: ')
		print "Project" + name
		#TODO

	# User doesn't have existing project
	elif existing_project.startswith("n"):
		print "\nIf you do not have a Google project you want to use, this script will generate a new one\nfor you."
		create_new_project = raw_input('\nWould you like to continue? (yes or no) ').lower()

		while not (create_new_project.startswith("y") or create_new_project.startswith("n")):
			create_new_project = raw_input('\nPlease answer yes or no: ').lower()

		# Create new project
		if create_new_project.startswith("y"):
			find_billing_accounts()
			name = raw_input("What do you want to call your project? ")
			create_google_project(name)

		# Don't create project, and exit
		elif create_new_project.startswith("n"):
			sys.exit("Exiting.")
			return




# Search for user's billing accounts
def find_billing_accounts():
	# from https://github.com/lukwam/gcp-tools/blob/master/lib/google.py#L216
	# create a request to list billingAccounts
    billing_accounts = billing.billingAccounts()
    request = billing_accounts.list()

    # create a list to hold all the projects
    billing_accounts_list = []

    # page through the responses
    while request is not None:

        # execute the request
        response = request.execute()

        # add projects to the projects list
        if 'billingAccounts' in response:
            billing_accounts_list.extend(response['billingAccounts'])

        request = billing_accounts.list_next(request, response)

    if len(billing_accounts_list) == 0:
    	# User does not have access to any billing accounts
    	print "You do not have a Google billing account set up. In order to run\nWDLs in the Google cloud you need an account to bill to. See the README\nfor more details.\nTo learn about creating a billing account, see here: \nhttps://cloud.google.com/billing/docs/how-to/manage-billing-account#create_a_new_billing_account"
        sys.exit("Exiting.")

    else:
    	print "\nYou have access to the following Google billing accounts: "
    	
    	# Setup table
    	headers = "Billing Account ID\tBilling Account Name"
    	print headers
    	print '-' * len(headers.expandtabs()) 

    	# Iterate and print every billing account 
    	for billing_acct in billing_accounts_list:
    		print "%s\t%s" % (billing_acct["name"].replace("billingAccounts/",""), billing_acct["displayName"])

    	print "\nEnter the \"Billing Account ID\" of the billing account you want to use\nto create a new Google project."
    	#TODO add note that this Google Project is where the compute will run, the billing account is what will be charged when user runs the sample script, or when they use the config. etc
    	billing_account_ID = raw_input("(IDs are case-sensitive and will look similar to this: 002481-B7351F-CD111E): ")
    	print "You have selected this Billing Account: %s" % billing_account_ID

    	# Project name with datetime stamp and user's email address
    	project_name = "cromwell_on_google-" + "%s" % (check_output(['gcloud', 'config', 'get-value', 'core/account']).rstrip().partition("@")[0]) + datetime.datetime.now().strftime("-datetime_%m_%d_%H_%M")
    	create_google_project(project_name, billing_account_ID)


#TODO Create a google project for the user
def create_google_project(project_name, billing_account_ID):
	# Create google project
	body = {'project_id': '%s' % project_name, 'name': '%s' % project_name}
	print body
	crm.projects().create(body=body).execute()


	# # Link new project to billing account
	# print "Linking project to your billing account..."


if __name__ == "__main__":
    main()

# varname = """
# File contents here, escaped properly

# """