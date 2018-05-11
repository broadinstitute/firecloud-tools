##!/usr/bin/env python
import sys, os

# Import GoogleCredentials
from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials
import google.auth
from oauth2client.file import Storage
from subprocess import check_output
from google.cloud import resource_manager, storage

# Import utilities
import json
import urllib
import datetime, time
import requests

# Setup of Google credentials necessary to access library functions
credentials = GoogleCredentials.get_application_default()
# Build a cloud billing API service
billing = build('cloudbilling', 'v1', credentials=credentials)
# Build a cloud resource manager API service
crm = build('cloudresourcemanager', 'v1', credentials=credentials)
# Create storage client
storage = build('storage', 'v1', credentials=credentials)
# Create Service management API service
smgt = build('servicemanagement', 'v1', credentials=credentials)

# Global variables
bucket_name = ""
project_name = ""
home = os.path.expanduser("~")

# The purpose of this script is to create a configuration file for Cromwell to run on Google Cloud with your local data.
def main():
	
	# Check user has not run script before, to avoid overwriting existing configuration file
	google_config_check()

	print "\nHello and welcome! This script is for users who want to run their WDL scripts on Google Cloud\nusing the Cromwell engine. This script will walk you through the following setup steps:\n(1) Check that you have Google Cloud SDK installed,\n(2) Select an existing Google Project or create a new one,\n(3) Create a Google Bucket for the workflow outputs,\n(4) Create a Configuration file for running Cromwell,\n(5) Enable APIs for running workflows,\n(6) Test your configuration file by downloading Cromwell and running a \"Hello, World\" WDL.\n\nReady? Let's get started.\n"

	# Ensure that gcloud SDK is installed, which is necessary to run the rest of the script. 
	sdk_install_check()

	# Select Google project (new or existing), continues on to create bucket
	which_google_project()

	# Create config
	create_config()

# Check user has not run script before, to avoid overwriting existing configuration file
def google_config_check():
	
	# Check for .google_cromwell.config in ~/
	existance = os.path.exists(home + '/.google_cromwell.config')
	
	# If there is a configuration file, exit
	if existance:
		print "\nYou already have a Cromwell configuration file. If you would like to clear this setup\nand create a new file, remove (or rename) the hidden file ~/.google_cromwell.config\n"
		sys.exit("Exiting.")

# Generic function for checking whether a user put in 'yes' or 'no', and loops until they do
def input_prompt(prompt_text):
	# Get user input after prompt
	yes_or_no = raw_input(prompt_text).lower()
	
	while not (yes_or_no.startswith("y") or yes_or_no.startswith("n")):
		yes_or_no = raw_input('\nPlease answer yes or no: ').lower()

	# Return boolean
	return yes_or_no.startswith("y")

# Ensure that gcloud SDK is installed, necessary to run the rest of the script. 
def sdk_install_check():
		
	# If gcloud SDK is not installed, install it.
	if os.system('gcloud version') is not 0:
		
		# Ask if user wants to install the SDK
		installSdk = input_prompt('\nStep (1): You do not have Google Cloud SDK installed, which you need to run this script.\nDo you want to install gcloud SDK? (yes or no)')
			
		# User chooses to install
		if installSdk:
			os.system('curl https://sdk.cloud.google.com | bash')
			shell = os.path.expanduser("$SHELL")
			# Need to create new shell to start using gcloud
			os.system('exec -l ' + shell)
			os.system('gcloud init')
			return
		
		# User chooses not to install SDK, exit the script because they can't continue.
		else:
			print "The Google Cloud SDK will not be installed. If you would like to install the SDK in the future, you can run this script again."
			sys.exit("Exiting.")
	
	# Gcloud SDK is already installed, and user can continue with setup
	else:
		print "\nStep (1): You already have Google Cloud SDK installed. Step (1) is complete."
		return

# Which Google Project to use (new or existing)
def which_google_project():
	global project_name

	existing_project = input_prompt('\n\nStep (2): Do you have an existing Google project where you want to run workflows? (yes or no) ')

	# User has existing project
	if existing_project:
		project_name = raw_input('\nEnter your Google project name: ')
		create_google_bucket()

	# User doesn't have existing project
	else:
		print "\nIf you do not have a Google project you want to use, this script will generate a new one for you."
		create_new_project = input_prompt('\nWould you like to continue? (yes or no) ')

		# Create new project
		if create_new_project:
			find_billing_accounts()

		# Don't create project, and exit
		else:
			print "\nYou can set up a Google Project outside of this script and then re-run the script.\nThen at step (2), select Yes that you have an existing project and enter the project name to continue with setup." 
			sys.exit("Exiting.")
	return project_name

# Search for user's billing accounts
def find_billing_accounts():
	
	# https://github.com/lukwam/gcp-tools/blob/master/lib/google.py#L216 
	# Create a request to list billingAccounts
    billing_accounts = billing.billingAccounts()
    request = billing_accounts.list()

    # Create a list to hold all the projects
    billing_accounts_list = []

    # Page through the responses
    while request is not None:

        # Execute the request
        response = request.execute()

        # Add projects to the projects list
        if 'billingAccounts' in response:
            billing_accounts_list.extend(response['billingAccounts'])

        # Create object of billing accounts
        request = billing_accounts.list_next(request, response)

    if len(billing_accounts_list) == 0:
    	# User does not have access to any billing accounts
    	print "You do not have a Google billing account set up. In order to run\nWDLs in the Google cloud you need an account to bill to. See the README\nfor more details.\nTo learn about creating a billing account, see here: \nhttps://cloud.google.com/billing/docs/how-to/manage-billing-account#create_a_new_billing_account"
        sys.exit("Exiting.")

    else:
    	# User does have access to more than 1 billing accounts
    	print "\nYou have access to the following Google billing accounts: "
    	
    	# Setup table of billing accounts
    	headers = "Billing Account ID\tBilling Account Name"
    	print headers
    	print '-' * len(headers.expandtabs()) 

    	# Iterate and print every billing account 
    	for billing_acct in billing_accounts_list:
    		print "%s\t%s" % (billing_acct["name"].replace("billingAccounts/",""), billing_acct["displayName"])

    	# Create table of Billing accounts
    	print "\nEnter the \"Billing Account ID\" of the billing account you want to use\nto create a new Google project. This will be the billing account that is charged\nfor storage and compute costs."
    	ex_billing_acct = "002481-B7351F-CD111E"
    	billing_account_id = raw_input("(IDs are case-sensitive and will look similar to this: %s): " % ex_billing_acct)
    	while len(billing_account_id) != len(ex_billing_acct):
			billing_account_id = raw_input("Please enter a valid billing account: ")
    	print "\nYou have selected this Billing Account: %s" % billing_account_id

    	# Project name with datetime stamp (minute and second) and user's email address
    	# DO NOT PUT AN UNDERSCORE '_' IN THE NAME. The name cannot be longer than 30 characters nor can it have the word 'google'
    	global project_name
    	user_name = check_output(['gcloud', 'config', 'get-value', 'core/account']).rstrip().partition("@")[0]
    	project_name = "cromwell-%s" % user_name + datetime.datetime.now().strftime("-%M-%S")
    	create_google_project(billing_account_id)
    	return project_name

# Create a google project for the user
def create_google_project(billing_account_id):
	global project_name

	# Create google project
	body = {'project_id': '%s' % project_name, 'name': '%s' % project_name, 'labels': None}
	crm.projects().create(body=body).execute()
	
	# Check the project is ready
	check_project_created()
	
	# Link new project to billing account
	enable_billing_account(billing_account_id)
	return project_name

def check_project_created():
	global project_name

	# List projects currently created
	result = crm.projects().list().execute()

	# Search through list of services to see if the project has been created  
	while True: 
		if "projects" in result:
			for s in result["projects"]:
				q = s["name"]
				if project_name in q:
					print "Project created successfully. View your new project here: https://console.cloud.google.com/home/dashboard?project=%s" % project_name
					return False
		print "Creating project..."
		time.sleep(10)
		result = crm.projects().list().execute() 

# Link the newly created Google project to the user's chosen billing account
def enable_billing_account(billing_account_id):
	global project_name

	# Setup parameters to update billing
	body = {"project_id": "%s" % project_name, "billing_account_name": "billingAccounts/%s" % billing_account_id, "billing_enabled": "True"}
	params = {"name": "projects/%s" % project_name, "body": body}
	
	# Enable billing account
	billing.projects().updateBillingInfo(**params).execute()

	# Check billing is enabled
	check_billing_enabled()

	# Then create bucket
	create_google_bucket()

def check_billing_enabled():
	global project_name

	# Setup parameters to check billing info
	params = {"name": "projects/%s" % project_name, "fields":"billingEnabled"}

	# Get current billing info
	result = billing.projects().getBillingInfo(**params).execute()

	# Check to see if the billing has been enabled  
	while True: 
		if "billingEnabled" in result:
			q = result["billingEnabled"]
			if True == q:
				print "Billing is enabled for your project."

				# A short pause before checking again 
				time.sleep(10)
				return False
		print "Linking project to your billing account..."
		time.sleep(10)
		result = billing.projects().getBillingInfo(**params).execute()

# Create a google bucket for the user
def create_google_bucket():
	global project_name
	global bucket_name

	print "Step (2) is complete.\n\n\nStep (3): Create a Google bucket, starting now..."

	# Setup parameters to create Google bucket
	bucket_name = "%s-executions" % project_name
	body = {"name": "%s" % bucket_name}
	params = {"project": "%s" % project_name, "body": body}

	# Create Google bucket
	storage.buckets().insert(**params).execute()

	# Check the bucket was created
	check_bucket_created(bucket_name)

	return bucket_name

# Confirm that the bucket has been created
def check_bucket_created(bucket_name):

	# Setup parameters to check bucket is created
	params = {"bucket": "%s" % bucket_name, "fields":"timeCreated"}
	
	# Check bucket was created
	result = storage.buckets().get(**params).execute()

	# Search for bucket in list of existing buckets
	while True: 
		if "timeCreated" in result:
			print "Bucket created successfully. View your new bucket here: https://console.cloud.google.com/storage/browser/%s" % bucket_name
			return False
		print "Creating bucket..."
		time.sleep(10)
		result = storage.buckets().get(**params).execute()

# Create the Cromwell configuration file
def create_config():
	
	global project_name
	global bucket_name
	print "Step (3) is complete.\n\nStep (4): Create configuration file, starting now..."

	config_contents = "include required(classpath(\"application\"))\n\ngoogle {\n\n\tapplication-name = \"cromwell\"\n\n\tauths = [\n\t\t{\n\t\t\tname = \"application-default\"\n\t\t\tscheme = \"application_default\"\n\t\t}\n\t]\n}\n\nengine {\n\tfilesystems {\n\t\tgcs {\n\t\t\tauth = \"application-default\"\n\t\t}\n\t}\n}\n\nbackend {\n\tdefault = \"JES\"\n\tproviders {\n\t\tJES {\n\t\t\tactor-factory = \"cromwell.backend.impl.jes.JesBackendLifecycleActorFactory\"\n\t\t\tconfig {\n\t\t\t\t// Google project\n\t\t\t\tproject = \"%s\"\n\t\t\t\tcompute-service-account = \"default\"\n\n\t\t\t\t// Base bucket for workflow executions\n\t\t\t\troot = \"gs://%s\"\n\n\t\t\t\t// Polling for completion backs-off gradually for slower-running jobs.\n\t\t\t\t// This is the maximum polling interval (in seconds):\n\t\t\t\tmaximum-polling-interval = 600\n\n\t\t\t\t// Optional Dockerhub Credentials. Can be used to access private docker images.\n\t\t\t\tdockerhub {\n\t\t\t\t\t// account = \"\"\n\t\t\t\t\t// token = \"\"\n\t\t\t\t}\n\n\t\t\t\tgenomics {\n\t\t\t\t\t// A reference to an auth defined in the \`google\` stanza at the top.  This auth is used to create\n\t\t\t\t\t// Pipelines and manipulate auth JSONs.\n\t\t\t\t\tauth = \"application-default\"\n\t\t\t\t\t// Endpoint for APIs, no reason to change this unless directed by Google.\n\t\t\t\t\tendpoint-url = \"https://genomics.googleapis.com/\"\n\t\t\t\t}\n\n\t\t\t\tfilesystems {\n\t\t\t\t\tgcs {\n\t\t\t\t\t\t// A reference to a potentially different auth for manipulating files via engine functions.\n\t\t\t\t\t\tauth = \"application-default\"\n\t\t\t\t\t}\n\t\t\t\t}\n\t\t\t}\n\t\t}\n\t}\n}" % (project_name, bucket_name)

	# Create configuration file
	with open(home + "/.google_cromwell.config","w+") as f:
		f.write(config_contents)

	print "Your configuration file is ready! It is stored in ~/.google_cromwell.config."
	start_cromwell_test()

# Test configuration and enable APIs
def start_cromwell_test():
	global project_name

	print "Step (4) is complete.\n\nStep (5): Enable APIs\nTo use your new configuration you will need to enable the following APIs in your Google project:\nGoogle Cloud Storage, Google Cloud Storage JSON, Google Compute Engine, Google Genomics."

	# Ask if user wants to enable APIs
	enable_apis = input_prompt('\nWould you like to enable these APIs now? (yes or no) ')
	
	# Enable APIs
	if enable_apis:
		serviceList = ["compute.googleapis.com", "storage-api.googleapis.com", "genomics.googleapis.com", "storage-component.googleapis.com"]
		for service_name in serviceList:
			enable_services(service_name)
		
		print "APIs are enabled. View the list of enabled APIs here: https://console.cloud.google.com/apis/dashboard?project=%s" % project_name
		
		# Continue with testing configuration
		continue_test = input_prompt('Step (5) is complete.\n\nStep (6): Test your configuration\nDo you want to run a Hello WDL test to check your configuration? (yes or no) ')

		if continue_test:
			hello_test()

		else:
			# User doesn't want to test config
			print "You are now ready to use Cromwell to run pipelines on Google Cloud.\nNext you can run a simple WDL with the Five Minute Tutorial here: http://cromwell.readthedocs.io/en/develop/tutorials/FiveMinuteIntro/\n"
			sys.exit("Exiting.")

	# Don't enable APIs and exit
	else:
		print "Don't forget to enable the APIs through the Google Console prior to using the configuration."
		sys.exit("Exiting.")

# Enable the APIs necessary for running genomics analysis
def enable_services(service_name):
	global project_name

	# Setup parameters to enable APIs
	body = {"consumerId": "project:%s" % project_name}
	params = {"serviceName": "%s" % service_name, "body": body}

	# Enable APIs
	smgt.services().enable(**params).execute()

	# Check that the APIs are enabled
	check_services_enabled(service_name)

# Check that the API is enabled before moving on to the next API
def check_services_enabled(service_name):
	global project_name

	# Setup parameters to check APIs
	params = {"consumerId": "project:%s" % project_name, "fields":"services/serviceName"}

	# List services currently enabled
	result = smgt.services().list(**params).execute()

	# Search through list of services to see if the API has been enabled  
	while True: 
		if "services" in result:
			for s in result["services"]:
				q = s["serviceName"]
				if service_name in q:
					return False
		print "Enabling APIs..."
		time.sleep(20)
		result = smgt.services().list(**params).execute()

# Download the latest Cromwell JAR and run the test
def hello_test():
	# Download latest Cromwell
	print "Downloading latest version of Cromwell execution engine..."

	# Get latest Cromwell release
	r = requests.get('https://api.github.com/repos/broadinstitute/cromwell/releases/latest')
	s = r.json()
	t = json.dumps(s)
	# Find exact URL to download the Cromwell JAR
	for asset in s["assets"]:
		if "cromwell-" in asset["browser_download_url"]:
			download_url = asset["browser_download_url"]
			urllib.urlretrieve(download_url, "cromwell.jar")

	# Run test
	test_configuration = "java -Dconfig.file=" + home +"/.google_cromwell.config -jar cromwell.jar run hello.wdl -i hello.inputs"
	print "Cromwell is downloaded and ready for operation.\n\nStarting Hello World test...\n\nRunning $ %s\n" % test_configuration
	os.system(test_configuration)

	# Success
	print "Workflow succeeded!\nOutputs for this workflow can be found in gs://%s\n\nYou have successfully set up your Google Project, Bucket, and configuration. \nCheck out the WDL website for more information on writing your own workflows: https://software.broadinstitute.org/wdl/documentation/quickstart.\n" % bucket_name

if __name__ == "__main__":
    main()