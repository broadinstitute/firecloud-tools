from oauth2client.client import GoogleCredentials
from oauth2client.client import AccessTokenRefreshError
import httplib2

from collections import defaultdict
import re
import multiprocessing as mp

from argparse import ArgumentParser
import googleapiclient.discovery
import boto
import math
import random
import os, sys, tempfile, subprocess
import json
import yaml
import csv
import xlrd
from xlrd.sheet import ctype_text
import re   
from firecloud import api
from google.cloud import bigquery
import operator

# http://stackoverflow.com/questions/29099404/ssl-insecureplatform-error-when-using-requests-package
import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()

import pprint 
pp = pprint.PrettyPrinter(indent=4)

import signal
import sys
from time import sleep

processes = []

def signal_handler(signal, frame):
    print('\n\n...Exiting...\n\n')
    for process in processes:
        process.kill()
        
    sleep(1)
        
    print('\n\nExited by user.')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)


global_error_message = """    ******************************************************************************
* ERROR: Unable to access your google credentials.                           *
*                                                                            *
* This is most often caused by either                                        *
*     (a) not having gcloud installed (see https://cloud.google.com/sdk/)    *
*     (b) gcloud is not logged in (run 'gcloud auth login')                  *
******************************************************************************"""

def fail(message):
    print "\n\nExiting -- %s" % message
    sys.exit(1)
    
def prompt_to_continue(msg):
    sys.stdout.write("\n%s [y/n]: " % msg)
    
    yes = set(['yes','y', 'ye', ''])
    no = set(['no','n'])

    choice = raw_input().lower()
    if choice in yes:
       return True
    elif choice in no:
       return False
    else:
       print "Please respond with 'yes' or 'no'"
       prompt_to_continue(msg)    