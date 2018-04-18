##!/usr/bin/env python
import sys, os#, google.cloud
from apiclient.discovery import build


# The purpose of this script is to create a configuration file for Cromwell to run on Google Cloud with your local data.

def main():
	#TODO port this to Python
	# (cd "$(dirname "$0")"
	
	# Ensure that the user has not run this script before, to avoid overwriting an existing configuration file
	google_config_check()

	print "\nHello and welcome! This script is for users who want to run their WDL scripts\non Google Cloud using the Cromwell engine. This script will walk you through\nthe steps for setting up your configuration file, and then it will test your\nconfiguration with a short workflow.\n\nReady? Let's get started.\n"

	# Ensure that gcloud SDK is installed, which is necessary to run the rest of the script. 
	sdk_install_check()

	# OAuth 2.0?
	# service = build('api_name', 'api_version', ...)

	# Select Google project (new or existing)
	which_google_project()
	


# Ensure that the user has not run this script before, to avoid overwriting an existing configuration file
def google_config_check():
	
	# Check for .google_cromwell.config in ~/
	home = os.path.expanduser("~")
	existance = os.path.exists(home + '/.google_cromwell.config')
	
	# If there is a configuration file, exit
	if existance == True:
		print "\nYou already have a Cromwell configuration file. If you would like to clear this setup\nand create a new file, remove (or rename) the hidden file ~/.google_cromwell.config\n"
		sys.exit("Exiting.")
	
	# Proceed with creating a config file
	else:
		return

# Ensure that gcloud SDK is installed, which is necessary to run the rest of the script. 
def sdk_install_check():
		
	# If gcloud SDK is not installed, install it.
	#TODO prints the gcloud version response; how to put it into the standard out/error/something else
	if os.system('gcloud version') is not 0:
		print "\nYou do not have Google Cloud SDK installed, which you need to run this script."
		
		# Ask if user wants to install the SDK
		installSdk = raw_input('Do you want to install gcloud SDK? (yes or no)').lower()
		
		while True:
			# User chooses to install
			if installSdk.startswith("y"):
				os.system('curl https://sdk.cloud.google.com | bash')
				shell = os.path.expanduser("$SHELL")
				#TODO what does next line do? Need to create new shell to start using gcloud
				os.system('exec -l' + shell) 
				#TODO fix errors, isn't installing glcoud properly
				os.system('gcloud init')
				break
			
			# User chooses not to install SDK, exit the script because they can't continue.
			elif installSdk.startswith("n"):
				sys.exit("Exiting.")
			
			# User didn't choose a word beginning with 'y' or 'n'.
			else: 
				print "Please answer yes or no."
	
	# Gcloud SDK is already installed, and user can continue with setup
	else:
		print "You have Google Cloud SDK installed."
		return

# Which Google Project to use (new or existing)
def which_google_project():

	existing_project = raw_input('\nDo you have an existing Google project where you want to run workflows? (yes or no) ').lower()

	while not (existing_project.startswith == "y" or existing_project.startswith == "n"):	
	# Check if user wants to use existing project
		existing_project = raw_input('\nPlease answer yes or no.').lower()
		
	# User has existing project
	if existing_project.startswith("y"):
		project = raw_input('\nEnter your Google project name: ')
		print "project " + project

	# User doesn't have existing project
	elif existing_project.startswith("n"):
		print "If you do not have a Google project you want to use, this script will generate a new one for you."
		create_new_project = raw_input('\nWould you like to continue? (yes or no) ').lower()

			# while True:
			# 	if not create_new_project.startswith("y") or create_new_project.startswith("n"):
			# 		"Please answer yes or no."

			# 	# Create new project
			#TODO 	elif create_new_project.startswith("y"):
			# 		name = raw_input("What do you want to call your project?")
			#		create_google_project(name)
			# 		break

			# 	# Don't create project, and exit
			# 	elif create_new_project.startswith("n"):
			# 		sys.exit("Exiting.")
			# 		break

#TODO Create a google project for the user
def create_google_project(name):




if __name__ == "__main__":
    main()

# varname = """
# File contents here, escaped properly

# """