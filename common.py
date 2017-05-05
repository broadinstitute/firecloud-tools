# Google/auth
from oauth2client.client import GoogleCredentials
from oauth2client.client import AccessTokenRefreshError
import httplib2
import googleapiclient.discovery
import boto
from google.cloud import bigquery
# http://stackoverflow.com/questions/29099404/ssl-insecureplatform-error-when-using-requests-package
import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()

# general
from argparse import ArgumentParser
from collections import defaultdict
import pandas
import re
import multiprocessing as mp
import math
import random
import tempfile
import os, sys, tempfile, subprocess
import operator
import signal
import sys
from time import sleep
from string import Template
import pprint
pp = pprint.PrettyPrinter(indent=4)


# data transformation
import json
import yaml
import csv
import xlrd
from xlrd.sheet import ctype_text

# firecloud python bindings
from firecloud import api as firecloud_api


processes = []


class AtomicCounter(object):
    def __init__(self, initval=0):
        self.val = mp.Value('i', initval)
        self.lock = mp.Lock()

    def increment(self):
        with self.lock:
            self.val.value += 1

    def value(self):
        with self.lock:
            return self.val.value

    def reset(self, value):
        self.val.value = value


def print_fields(obj):
    for item in vars(obj).items():
        print item


class DefaultArgsParser:
    def __init__(self, description):
        self.parser = ArgumentParser(description=description)

        self.parser.add_argument('-u', '--url', dest='fc_url', action='store', required=False,
                            help='If set, this will override which api is used (default is https://api.firecloud.org/api)')

    def __getattr__(self, attr):
        return self.parser.__getattribute__(attr)

    def parse_args(self):
        args = self.parser.parse_args()

        if args.fc_url:
            firecloud_api.PROD_API_ROOT = args.fc_url

        return args

def list_to_dict(input_list, key_fcn, value_fcn=lambda item: item, transform_fcn=lambda item: item):
    dicted_list = defaultdict(list)

    for item in input_list:
        key = key_fcn(item)
        dicted_list[key].append(value_fcn(item))

    return dicted_list

def printj(*args):
    print joins(*args)
def joins(*args):
    return ''.join(args)


# simple wrapper to do substitution from variables local to caller function using the
# ${some_variable_name} syntax in the string - T itself is a substituted string
class T(str):
    def __new__(cls, string):
        import inspect
        frame = inspect.currentframe()
        try:
            return super(T, cls).__new__(cls, Template(string).substitute(frame.f_back.f_locals))
        finally:
            del frame

# take in a google bucket url for a file that was output by a submission and break it
# into its various parts (submission id, workflow id, etc)
class SubmissionOutput:
    def __init__(self, file_path):
        # default to no value for all fields unless otherwise set
        self.bucket_name = None
        self.submission_id = None
        self.workflow_name = None
        self.workflow_id = None
        self.task_name = None
        self.task_file_path = None
        self.file_path = file_path

        try:
            # gs://<group 1: bucket name>/<group 2: submission id>/<group 3: workflow name>/<group 4: workflow id>/<group 5: task name>/<group 6: task file path>
            components = re.search(r"gs://([^/]+)/([^/]+)/([^/]+)/([^/]+)/([^/]+)/(.+)", file_path)

            self.bucket_name = components.group(1)
            self.submission_id = components.group(2)
            self.workflow_name = components.group(3)
            self.workflow_id = components.group(4)
            self.task_name = components.group(5)
            self.task_file_path = components.group(6)
        except:
            #print "Unable to make SubmissionOutput from URL:",file_path
            pass


def human_file_size_fmt(num, suffix='B'):
    num_float = float(num)
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num_float) < 1024.0:
            return "%3.1f%s%s" % (num_float, unit, suffix)
        num_float /= 1024.0
    return "%.1f%s%s" % (num_float, 'Yi', suffix)


def create_service():
    """Creates the service object for calling the Cloud Storage API."""
    # Construct the service object for interacting with the Cloud Storage API -
    # the 'storage' service, at version 'v1'.
    # You can browse other available api services and versions here:
    #     https://developers.google.com/api-client-library/python/apis/
    return googleapiclient.discovery.build('storage', 'v1')


def setup():
    credentials = GoogleCredentials.get_application_default()
    print "Using Google client id:", credentials.client_id


def get_workflow_metadata(namespace, name, submission_id, workflow_id, *include_keys):
    headers = firecloud_api._fiss_access_headers()

    include_key_string = "includeKey=%s&" % ("%2C%20".join(list(include_keys))) if include_keys else ""
    uri = "{0}/workspaces/{1}/{2}/submissions/{3}/workflows/{4}?&{5}expandSubWorkflows=false".format(
        firecloud_api.PROD_API_ROOT, namespace, name, submission_id, workflow_id, include_key_string)

    return requests.get(uri, headers=headers).json()

class ProgressBar:
    def __init__(self, min, max, description="", num_tabs=0):
        self.val = mp.Value('i', min)
        self.lock = mp.Lock()

        self.min = min
        self.max = max
        self.description = description
        self.num_tabs = num_tabs

    def print_bar(self):
        percent = float(self.val.value - self.min) / (self.max - self.min)
        width = 50

        bar_width = int(math.ceil(width * percent))
        print "\r%s[%s%s] %s/%s %s" % (
        "\t" * self.num_tabs, "=" * bar_width, " " * (width - bar_width), self.val.value, self.max, self.description),

        if self.val.value >= max:
            print "\n"

        sys.stdout.flush()

    def increment(self):
        self.val.value += 1

def print_progress_bar(value, min, max, description="", num_tabs=0):
    percent = float(value-min) / (max-min)
    width = 50

    bar_width = int(math.ceil(width * percent))
    print "\r%s[%s%s] %s/%s %s" % ("\t"*num_tabs, "="*bar_width, " "*(width-bar_width), value, max, description),

    if value >= max:
        print "\n"

    sys.stdout.flush()


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


# def make_fc_request(function, *args):
#     request = firecloud_api

def get_entity_by_page(namespace, name, entity_type, page, page_size):
    headers = firecloud_api._fiss_access_headers()

    uri = "{0}/workspaces/{1}/{2}/entityQuery/{3}?page={4}&pageSize={5}".format(
        firecloud_api.PROD_API_ROOT, namespace, name, entity_type, page, page_size)

    return requests.get(uri, headers=headers).json()


def get_all_bound_file_paths(ws_namespace, ws_name):
    request = firecloud_api.list_entity_types(ws_namespace, ws_name)
    if request.status_code != 200:
        fail(request.text)

    entity_types_json = request.json()
    file_path_to_entities = defaultdict(list)
    referenced_file_paths_in_workspace = []

    #
    for entity_type in entity_types_json:
        entity_count = entity_types_json[entity_type]["count"]

        page_size = 1000
        num_pages = int(math.ceil(float(entity_count) / page_size))
        for i in range(1, num_pages+1):
            for entity_json in get_entity_by_page(ws_namespace, ws_name, entity_type, i, page_size)["results"]:
                for attribute_value in entity_json["attributes"].values():
                        if re.match(r"gs://", str(attribute_value)):
                            referenced_file_paths_in_workspace.append(attribute_value)
                            file_path_to_entities[attribute_value].append(entity_json)
    return file_path_to_entities


def get_entity_by_page(namespace, name, entity_type, page, page_size):
    headers = firecloud_api._fiss_access_headers()

    uri = "{0}/workspaces/{1}/{2}/entityQuery/{3}?page={4}&pageSize={5}".format(
        firecloud_api.PROD_API_ROOT, namespace, name, entity_type, page, page_size)

    return requests.get(uri, headers=headers).json()


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