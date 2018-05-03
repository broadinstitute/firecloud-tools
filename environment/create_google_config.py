##!/usr/bin/env python
import sys, os
from apiclient.discovery import build
# import GoogleCredentials
from oauth2client.client import GoogleCredentials
import google.auth
from subprocess import check_output
from google.cloud import resource_manager, storage
import json, re
import urllib
import datetime, time
import requests

# Google setup
credentials = GoogleCredentials.get_application_default()
# Build a cloud billing API service
billing = build('cloudbilling', 'v1', credentials=credentials)
# Build a cloud resource manager API service
crm = build('cloudresourcemanager', 'v1', credentials=credentials)
# Create storage client
storage_client = storage.Client()
# Create Service management API service
smgt = build('servicemanagement', 'v1', credentials=credentials)

# Global variables
project_name = ""
bucket_name = ""

#TODO: take out the error messages, info prompts, etc and put into another file (import them here)
#TODO: consider making the configuration a class, so that a user could create multiple configurations
#TODO: add nice error message for if user does keyboard interrupt (command + C) at any point in the script. Is it possible to make the errors specific to where they are in the process? post-created project, post-config created, etc?

# The purpose of this script is to create a configuration file for Cromwell to run on Google Cloud with your local data.

def main():
	
	# Ensure that the user has not run this script before, to avoid overwriting an existing configuration file
	google_config_check()

	print "\nHello and welcome! This script is for users who want to run their WDL scripts on Google Cloud\nusing the Cromwell engine. This script will walk you through the following setup steps:\n(1) Check that you have Google Cloud SDK installed,\n(2) Select an existing Google Project or create a new one,\n(3) Create a Google Bucket for the workflow outputs,\n(4) Create a Configuration file for running Cromwell,\n(5) Enable APIs for running workflows,\n(6) Test your configuration file by downloading Cromwell and running a \"Hello, World\" WDL.\n\nReady? Let's get started.\n"

	# Ensure that gcloud SDK is installed, which is necessary to run the rest of the script. 
	sdk_install_check()

	# Select Google project (new or existing)
	which_google_project()

	# Create config
	create_config()

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
		installSdk = raw_input('\nStep (1): You do not have Google Cloud SDK installed, which you need to run this script.\nDo you want to install gcloud SDK? (yes or no)').lower()
		
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
			print "The Google Cloud SDK will not be installed. If you would like to install the SDK in the future, you can run this script again."
			sys.exit("Exiting.")
	
	# Gcloud SDK is already installed, and user can continue with setup
	else:
		print "\nStep (1): You already have Google Cloud SDK installed. Step (1) is complete."
		return

# Which Google Project to use (new or existing)
def which_google_project():

	existing_project = raw_input('\n\nStep (2): Do you have an existing Google project where you want to run workflows? (yes or no) ').lower()

	while not (existing_project.startswith("y") or existing_project.startswith("n")):	
		existing_project = raw_input('\nPlease answer yes or no: ').lower()
		
	# User has existing project
	if existing_project.startswith("y"):
		global project_name
		project_name = raw_input('\nEnter your Google project name: ')
		create_google_bucket(project_name)
		return project_name

	# User doesn't have existing project
	elif existing_project.startswith("n"):
		print "\nIf you do not have a Google project you want to use, this script will generate a new one for you."
		create_new_project = raw_input('\nWould you like to continue? (yes or no) ').lower()

		while not (create_new_project.startswith("y") or create_new_project.startswith("n")):
			create_new_project = raw_input('\nPlease answer yes or no: ').lower()

		# Create new project
		if create_new_project.startswith("y"):
			find_billing_accounts()
			# Which later creates the google project

		# Don't create project, and exit
		elif create_new_project.startswith("n"):
			print "\nYou can set up a Google Project outside of this script and then re-run the script.\nThen at step (2), select Yes that you have an existing project and enter the project name to continue with setup." 
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
    	billing_account_id = raw_input("(IDs are case-sensitive and will look similar to this: 002481-B7351F-CD111E): ")
    	print "You have selected this Billing Account: %s" % billing_account_id

    	# Project name with datetime stamp (minute and second) and user's email address
    	# DO NOT PUT AN UNDERSCORE '_' IN THE NAME, it cannot be longer than 30 characters, nor can it have "google"
    	global project_name
    	user_name = check_output(['gcloud', 'config', 'get-value', 'core/account']).rstrip().partition("@")[0]
    	project_name = "cromwell-%s" % user_name + datetime.datetime.now().strftime("-%M-%S")
    	create_google_project(project_name, billing_account_id)

# Create a google project for the user
def create_google_project(project_name, billing_account_id):
	# Create google project
	print "Creating Google project..."
	body = {'project_id': '%s' % project_name, 'name': '%s' % project_name, 'labels': None}
	crm.projects().create(body=body).execute()
	# Sleep is necessary to give time to create project
	#TODO decrease time?
	time.sleep(10) 
	# Link new project to billing account
	print "Linking project to your billing account..."
	enable_billing_account(billing_account_id, project_name)
	return project_name

# Link the newly created Google project to the user's chosen billing account
def enable_billing_account(billing_account_id, project_name):
	body = {"project_id": "%s" % project_name, "billing_account_name": "billingAccounts/%s" % billing_account_id, "billing_enabled": "True"}
	params = {"name": "projects/%s" % project_name, "body": body}
	
	# Enable billing account
	billing.projects().updateBillingInfo(**params).execute()
	print "Project created successfully. View your new project here: https://console.cloud.google.com/home/dashboard?project=%s" % project_name
	create_google_bucket(project_name)

def create_google_bucket(project_name):
	#TODO: change bucket name to not include datetime
	#TODO: handle exception if existing bucket
	print "Step (2) is complete.\n\nStep (3): Create a Google bucket, starting now..."
	global bucket_name
	bucket_name = "%s-executions" % project_name
	storage_client.create_bucket(bucket_name)
	print "Bucket created successfully. View your new bucket here: https://console.cloud.google.com/storage/browser/%s" % bucket_name
	return bucket_name

#TODO: ask for dockerhub credentials if they are going to use private dockers

def create_config():
	#TODO: make `home` a global variable to remove duplication from inital check for existing config file
	print "Step (3) is complete.\n\nStep (4): Create configuration file, starting now..."
	home = os.path.expanduser("~")
	config = open(home + "/.google_cromwell.config","w+")
	#TODO: make tabs smaller
	#TODO: put contents of config file into another file, automate way to take formatting and make it into string format
	config_contents = "include required(classpath(\"application\"))\n\ngoogle {\n\n\tapplication-name = \"cromwell\"\n\n\tauths = [\n\t\t{\n\t\t\tname = \"application-default\"\n\t\t\tscheme = \"application_default\"\n\t\t}\n\t]\n}\n\nengine {\n\tfilesystems {\n\t\tgcs {\n\t\t\tauth = \"application-default\"\n\t\t}\n\t}\n}\n\nbackend {\n\tdefault = \"JES\"\n\tproviders {\n\t\tJES {\n\t\t\tactor-factory = \"cromwell.backend.impl.jes.JesBackendLifecycleActorFactory\"\n\t\t\tconfig {\n\t\t\t\t// Google project\n\t\t\t\tproject = \"%s\"\n\t\t\t\tcompute-service-account = \"default\"\n\n\t\t\t\t// Base bucket for workflow executions\n\t\t\t\troot = \"gs://%s\"\n\n\t\t\t\t// Polling for completion backs-off gradually for slower-running jobs.\n\t\t\t\t// This is the maximum polling interval (in seconds):\n\t\t\t\tmaximum-polling-interval = 600\n\n\t\t\t\t// Optional Dockerhub Credentials. Can be used to access private docker images.\n\t\t\t\tdockerhub {\n\t\t\t\t\t// account = \"\"\n\t\t\t\t\t// token = \"\"\n\t\t\t\t}\n\n\t\t\t\tgenomics {\n\t\t\t\t\t// A reference to an auth defined in the \`google\` stanza at the top.  This auth is used to create\n\t\t\t\t\t// Pipelines and manipulate auth JSONs.\n\t\t\t\t\tauth = \"application-default\"\n\t\t\t\t\t// Endpoint for APIs, no reason to change this unless directed by Google.\n\t\t\t\t\tendpoint-url = \"https://genomics.googleapis.com/\"\n\t\t\t\t}\n\n\t\t\t\tfilesystems {\n\t\t\t\t\tgcs {\n\t\t\t\t\t\t// A reference to a potentially different auth for manipulating files via engine functions.\n\t\t\t\t\t\tauth = \"application-default\"\n\t\t\t\t\t}\n\t\t\t\t}\n\t\t\t}\n\t\t}\n\t}\n}" % (project_name, bucket_name)
	config.write(config_contents)
	config.close()
	print "Your configuration file is ready! It is stored in ~/.google_cromwell.config."
	start_cromwell_test()

def start_cromwell_test():
	print "Step (4) is complete.\n\nStep (5): Enable APIs\nTo use your new configuration you will need to enable the following APIs in your Google project:\nGoogle Cloud Storage, Google Compute Engine, Google Genomics."
	enable_apis = raw_input('\nWould you like to enable these APIs now? (yes or no) ').lower()
	
	while not (enable_apis.startswith("y") or enable_apis.startswith("n")):
		enable_apis = raw_input('\nPlease answer yes or no: ').lower()
	
	# Enable APIs
	if enable_apis.startswith("y"):
		print "\nEnabling APIs..."
		serviceList = ["compute.googleapis.com", "genomics.googleapis.com", "storage-component.googleapis.com"]
		for serviceName in serviceList:
			enable_services(serviceName)
		print "APIs are enabled. View the list of enabled APIs here: https://console.cloud.google.com/apis/dashboard?project=%s" % project_name
		
		# Continue with testing configuration
		continue_test = raw_input('Step (5) is complete.\n\nStep (6): Test your configuration\nDo you want to run a Hello WDL test to check your configuration? (yes or no) ')
			
		while not (continue_test.startswith("y") or continue_test.startswith("n")):
			continue_test = raw_input('\nPlease answer yes or no: ').lower()

		if continue_test.startswith("y"):
			hello_test()

		elif continue_test.startswith("n"):
			sys.exit("Exiting.")
			return

	# Don't enable APIs, and exit
	elif enable_apis.startswith("n"):
		print "Don't forget to enable the APIs through the Google Console or gcloud SDK prior to using the configuration."
		sys.exit("Exiting.")
		return

def enable_services(serviceName):
	body = {"consumerId": "project:%s" % project_name}
	params = {"serviceName": "%s" % serviceName, "body": body}
	# print params
	smgt.services().enable(**params).execute()

def hello_test():
	# Create WDL
	print "Creating WDL file..."
	wdl_ex = open("hello.wdl","w+")
	#TODO: make tabs smaller
	wdl_contents = "task hello {\n\tString addressee\n\tcommand {\n\t\techo \"Hello ${addressee}! Welcome to Cromwell . . . on Google Cloud!\"\n\t}\n\toutput {\n\t\tString message = read_string(stdout())\n\t}\n\truntime {\n\t\tdocker: \"ubuntu:latest\"\n\t}\n}\n\nworkflow wf_hello {\n\tcall hello\n\n\toutput {\n\t\thello.message\n\t}\n}"
	wdl_ex.write(wdl_contents)
	wdl_ex.close()
	print "Your WDL file is ready! It is stored as hello.wdl."

	# Create Inputs file
	print "Creating inputs file..."
	inputs_to_wdl = open("hello.inputs", "w+")
	#TODO: make tabs smaller
	#TODO: check \ necessary?
	inputs_contents = "{\n\t\"wf_hello.hello.addressee\": \"World\"\n}"
	inputs_to_wdl.write(inputs_contents)
	inputs_to_wdl.close()
	print "Your inputs file is ready! It is stored as hello.inputs."

	# Download latest Cromwell
	print "Downloading latest version of Cromwell execution engine..."
	r = requests.get('https://api.github.com/repos/broadinstitute/cromwell/releases/latest')
	s = r.json()
	t = json.dumps(s)
	for asset in s["assets"]:
		if "cromwell-" in asset["browser_download_url"]:
			download_url = asset["browser_download_url"]
			urllib.urlretrieve(download_url, "cromwell.jar")

	#TODO: add error handling for if cromwell doesn't download

	# Run test
	home = os.path.expanduser("~")
	test_configuration = "java -Dconfig.file=" + home +"/.google_cromwell.config -jar cromwell.jar run hello.wdl -i hello.inputs"
	print "Cromwell is downloaded and ready for operation.\n\nStarting Hello World test.\n\nRunning $ %s" % test_configuration
	os.system(test_configuration)

	# Success
	print "Workflow succeeded!\nOutputs for this workflow can be found in gs://%s\n\nYou have successfully set up your Google Project, Bucket, and configuration. \nCheck out the WDL website for more information on writing your own workflows: https://software.broadinstitute.org/wdl/documentation/quickstart.\n Happy WDL-ing!" % bucket_name

if __name__ == "__main__":
    main()