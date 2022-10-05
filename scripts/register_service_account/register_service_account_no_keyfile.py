##!/usr/bin/env python
# This version is modified from the original register_service_account.py.
# The intent is that you run this script AS the service account,
# therefore bypassing the need for a json keyfile
# For details (internal), see https://docs.google.com/document/d/1OH5m_eoT7PIZNGLz1_T3Kscp4IS7muWDSxbInUgF3d4/edit?resourcekey=0-kvYes7f0ZJoI44TOk-dUXA#
from argparse import ArgumentParser
from google.auth.transport.requests import AuthorizedSession
import firecloud.api as fapi
import google.auth
import json

def main():
    # The main argument parser
    parser = ArgumentParser(description="Register a service account for use in FireCloud.")

    # Core application arguments
    parser.add_argument('-e', '--owner_email', dest='owner_email', action='store', required=True, help='Email address of the person who owns this service account')
    parser.add_argument('-u', '--url', dest='fc_url', action='store', default="https://api.firecloud.org", required=False, help='Base url of FireCloud server to contact (Default Prod URL: "https://api.firecloud.org", Dev URL: "https://firecloud-orchestration.dsde-dev.broadinstitute.org")')

    # Additional arguments
    parser.add_argument('-f', '--first_name', dest='first_name', action='store', default="None", required=False, help='First name to register for user')
    parser.add_argument('-l', '--last_name', dest='last_name', action='store', default="None", required=False, help='Last name to register for user')

    args = parser.parse_args()

    scopes = ['https://www.googleapis.com/auth/userinfo.profile', 'https://www.googleapis.com/auth/userinfo.email']
    credentials, project = google.auth.default(scopes=scopes)
    authed_session = AuthorizedSession(credentials)

    headers = {"User-Agent": fapi.FISS_USER_AGENT}

    uri = args.fc_url + "/register/profile"

    profile_json = {"firstName": args.first_name,
                    "lastName": args.last_name,
                    "title": "None",
                    "contactEmail": args.owner_email,
                    "institute": "None",
                    "institutionalProgram": "None",
                    "programLocationCity": "None",
                    "programLocationState": "None",
                    "programLocationCountry": "None",
                    "pi": "None",
                    "nonProfitStatus": "false"}
    response = authed_session.request('POST', uri, headers=headers, data=json.dumps(profile_json))

    if response.status_code == 200:
        print("The service account %s is now registered with FireCloud. You can share workspaces with this address, or use it to call APIs." % credentials.service_account_email)
    else:
        print("Unable to register service account: %s" % response.text)

if __name__ == "__main__":
    main()