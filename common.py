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
import dateutil.parser

# firecloud python bindings
from firecloud import api as firecloud_api
from firecloud.fccore import __fcconfig as fcconfig


processes = []


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
            "\t" * self.num_tabs, "=" * bar_width, " " * (width - bar_width), self.val.value, self.max,
            self.description),

        if self.val.value >= max:
            print "\n"

        sys.stdout.flush()

    def increment(self):
        self.val.value += 1


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
            set_fc_url(args.fc_url)
        return args


def list_to_dict(input_list, key_fcn, value_fcn=lambda item: item, transform_fcn=lambda item: item):
    dicted_list = defaultdict(list)

    for item in input_list:
        key = key_fcn(item)
        dicted_list[key].append(value_fcn(item))

    return dicted_list


def get_access_token():
    return GoogleCredentials.get_application_default().get_access_token().access_token

# only needed until firecloud python library in pypi supports service accounts
def _fiss_access_headers_local(headers=None):
    """ Return request headers for fiss.
        Retrieves an access token with the user's google crededentials, and
        inserts FISS as the User-Agent.
    Args:
        headers (dict): Include additional headers as key-value pairs
    """
    credentials = GoogleCredentials.get_application_default()
    credentials = credentials.create_scoped(['https://www.googleapis.com/auth/userinfo.profile', 'https://www.googleapis.com/auth/userinfo.email'])
    access_token = credentials.get_access_token().access_token
    fiss_headers = {"Authorization" : "bearer " + access_token}
    fiss_headers["User-Agent"] = firecloud_api.FISS_USER_AGENT
    if headers:
        fiss_headers.update(headers)
    return fiss_headers


def setup():
    firecloud_api._fiss_access_headers = _fiss_access_headers_local
    registration_info = requests.get("https://api.firecloud.org/register", headers=firecloud_api._fiss_access_headers())
    if registration_info.status_code == 404:
        fail("This account is not registered with FireCloud.")

    print "Using credentials for firecloud account:", registration_info.json()["userInfo"]["userEmail"]


def get_workflow_metadata(namespace, name, submission_id, workflow_id, *include_keys):
    headers = firecloud_api._fiss_access_headers()

    include_key_string = "includeKey=%s&" % ("%2C%20".join(list(include_keys))) if include_keys else ""
    uri = "{0}workspaces/{1}/{2}/submissions/{3}/workflows/{4}?&{5}expandSubWorkflows=false".format(
        get_fc_url(), namespace, name, submission_id, workflow_id, include_key_string)

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


def get_entity_by_page(namespace, name, entity_type, page, page_size):
    headers = firecloud_api._fiss_access_headers()

    uri = "{0}/workspaces/{1}/{2}/entityQuery/{3}?page={4}&pageSize={5}".format(
        get_fc_url(), namespace, name, entity_type, page, page_size)

    return requests.get(uri, headers=headers).json()


def get_all_bound_file_paths(ws_namespace, ws_name):
    request = firecloud_api.list_entity_types(ws_namespace, ws_name)
    if request.status_code != 200:
        fail(request.text)

    entity_types_json = request.json()
    attribute_name_for_url_to_entity_json = defaultdict(list)
    referenced_file_paths_in_workspace = []

    #
    for entity_type in entity_types_json:
        entity_count = entity_types_json[entity_type]["count"]

        page_size = 1000
        num_pages = int(math.ceil(float(entity_count) / page_size))
        for i in range(1, num_pages+1):
            for entity_json in get_entity_by_page(ws_namespace, ws_name, entity_type, i, page_size)["results"]:
                for attribute_name, attribute_value in entity_json["attributes"].items():
                        if re.match(r"gs://", str(attribute_value)):
                            referenced_file_paths_in_workspace.append(attribute_value)
                            attribute_name_for_url_to_entity_json[attribute_name].append(entity_json)
    return attribute_name_for_url_to_entity_json


def set_fc_url(url):
    fcconfig.root_url = url


def get_fc_url():
    return fcconfig.root_url


def get_entity_by_page(namespace, name, entity_type, page, page_size):
    headers = firecloud_api._fiss_access_headers()

    uri = "{0}workspaces/{1}/{2}/entityQuery/{3}?page={4}&pageSize={5}".format(
        get_fc_url(), namespace, name, entity_type, page, page_size)

    return requests.get(uri, headers=headers).json()


def get_workspace_storage_estimate(namespace, name):
    headers = firecloud_api._fiss_access_headers()

    uri = "{0}/workspaces/{1}/{2}/storageCostEstimate".format(
        get_fc_url(), namespace, name)

    return requests.get(uri, headers=headers)


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