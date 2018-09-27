import sys
import os
from oauth2client.client import GoogleCredentials


if len(sys.argv) > 1 and os.path.isfile(sys.argv[1]):
    from oauth2client.service_account import ServiceAccountCredentials
    scopes = ['https://www.googleapis.com/auth/userinfo.profile', 'https://www.googleapis.com/auth/userinfo.email']
    if len(sys.argv) > 2:
      newScopes = eval(sys.argv[2])
      if isinstance(newScopes, list):
        scopes = scopes + newScopes
      else:
        print "If you want to add additional scopes, add this in the form of a list."
        print "For example: " \
              "\n\tpython get_access_token.py <credentials/default> \"['https://www.googleapis.com/auth/cloud-platform']\""
        exit(1)
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        sys.argv[1], scopes=scopes)
elif len(sys.argv) > 1 and sys.argv[1] == "default":
    credentials = GoogleCredentials.get_application_default()
else:
    print "This script will print out an access token for either the application default credentials or for a service account.  " \
          "\nOnly the token is printed so this can be combined with other commands such as curl calls."
    print "Usage: " \
          "\n\tpython get_access_token.py default (to get access token for the application default credentials)" \
          "\n\tpython get_access_token.py <path to service account json file> (to get access token for a service account)"
    exit(1)

print credentials.get_access_token().access_token
