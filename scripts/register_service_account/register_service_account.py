##!/usr/bin/env python
from common import *


def main():
    # The main argument parser
    parser = DefaultArgsParser(description="Register a service account for use in FireCloud.")

    # Core application arguments
    parser.add_argument('-j', '--json_credentials', dest='json_credentials', action='store', required=True, help='Path to the json credentials file for this service account.')
    parser.add_argument('-e', '--owner_email', dest='owner_email', action='store', required=True, help='Email address of the person who owns this service account')

    args = parser.parse_args()

    from oauth2client.service_account import ServiceAccountCredentials
    scopes = ['https://www.googleapis.com/auth/userinfo.profile', 'https://www.googleapis.com/auth/userinfo.email']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(args.json_credentials, scopes=scopes)
    headers = {"Authorization": "bearer " + credentials.get_access_token().access_token}
    headers["User-Agent"] = firecloud_api.FISS_USER_AGENT

    uri = args.fc_url+"/register/profile" if args.fc_url else "https://api.firecloud.org/register/profile"

    profile_json = {"firstName":"None", "lastName": "None", "title":"None", "contactEmail":args.owner_email,
                               "institute":"None", "institutionalProgram": "None", "programLocationCity": "None", "programLocationState": "None",
                               "programLocationCountry": "None", "pi": "None", "nonProfitStatus": "false"}
    request = requests.post(uri, headers=headers, json=profile_json)

    if request.status_code == 200:
        print "The service account %s is now registered with FireCloud.  You can share workspaces with this address, or use it to call APIs." % credentials._service_account_email
    else:
        fail("Unable to register service account: %s" % request.text)

if __name__ == "__main__":
    main()
