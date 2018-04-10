##!/usr/bin/env python
#from common import *
import sys, os

# The purpose of this script is to create a configuration file for Cromwell to run on Google Cloud with your local data.

def main():
	# Set system option
	os.system('set -e')

	# Disable update check
	os.system('gcloud config set component_manager/disable_update_check true')

	# port this to Python
	# (cd "$(dirname "$0")"

	# Check whether an existing .google_cromwell.config exists
	home = os.path.expanduser("~")
	print home
	existance = os.path.exists(home + '/.google_cromwell.config')
	print existance

	# Config file does not exisit, proceed with setup
	print "\nHello and welcome! This script is for users who want to run their WDL scripts\non Google Cloud using the Cromwell engine. This script will walk you through\nthe steps for setting up your configuration file, and then it will test your\nconfiguration with a short workflow.\nReady? Let's get started."

	# Check if gcloud SDK is installed
	os.system('gcloud version | grep -q "gcloud: command not found"')


	# Which Google Project to use (new or existing)


if __name__ == "__main__":
    main()