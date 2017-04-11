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
from firecloud import api as firecloud_api
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

import tempfile

processes = []

def print_fields(obj):
    for item in vars(obj).items():
        print item


def setup():
    credentials = GoogleCredentials.get_application_default()
    print "Using Google client id:", credentials.client_id


def get_workflow_metadata(namespace, name, submission_id, workflow_id):
    headers = firecloud_api._fiss_access_headers()
    uri = "{0}/workspaces/{1}/{2}/submissions/{3}/workflows/{4}".format(
        firecloud_api.PROD_API_ROOT, namespace, name, submission_id, workflow_id)

    return requests.get(uri, headers=headers).json()


def signal_handler(signal, frame):
    print('\n\n...Exiting...\n\n')
    for process in processes:
        process.kill()
        
    sleep(1)
        
    print('\n\nExited by user.')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)


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