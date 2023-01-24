##!/usr/bin/env python
from common import *
from argparse import ArgumentParser

def main():
    # The main argument parser
    parser = ArgumentParser(description="Register a service account for use in FireCloud.")

    # Core application arguments
    parser.add_argument('-j', '--json_credentials', dest='json_credentials', action='store', help='Path to the json credentials file for this service account.')
    parser.add_argument('-e', '--owner_email', dest='owner_email', action='store', required=True, help='Email address of the person who owns this service account')
    parser.add_argument('-u', '--url', dest='fc_url', action='store', default="https://api.firecloud.org", required=False, help='Base url of FireCloud server to contact (Default Prod URL: "https://api.firecloud.org", Dev URL: "https://firecloud-orchestration.dsde-dev.broadinstitute.org")')

    # Additional arguments
    parser.add_argument('-f', '--first_name', dest='first_name', action='store', default="None", required=False, help='First name to register for user')
    parser.add_argument('-l', '--last_name', dest='last_name', action='store', default="None", required=False, help='Last name to register for user')

    args = parser.parse_args()
    
    if args.json_credentials:
        from oauth2client.service_account import ServiceAccountCredentials
        scopes = ['https://www.googleapis.com/auth/userinfo.profile', 'https://www.googleapis.com/auth/userinfo.email']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(args.json_credentials, scopes=scopes)
        access_token = credentials.get_access_token().access_token
    else:
        print('-j / --json_credentials was not provided. Attempting to contact metadata.google.internal. This will only work from a GCP VM.')
        resp = requests.get('http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token',
                            headers={'Metadata-Flavor': 'Google'})
        resp.raise_for_status()
        access_token = resp.json()['access_token']
        
    headers = {"Authorization": "bearer " + access_token}
    headers["User-Agent"] = firecloud_api.FISS_USER_AGENT

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
    request = requests.post(uri, headers=headers, json=profile_json)

    if request.status_code == 200:
        print("The service account %s is now registered with FireCloud. You can share workspaces with this address, or use it to call APIs." % credentials._service_account_email)
    else:
        fail("Unable to register service account: %s" % request.text)

if __name__ == "__main__":
    main()
