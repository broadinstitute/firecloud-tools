import sys
import os
from oauth2client.client import GoogleCredentials
import requests


credentials = GoogleCredentials.get_application_default()

print "Access token: " + credentials.get_access_token().access_token
print requests.get("https://www.googleapis.com/oauth2/v1/tokeninfo?access_token=" + credentials.get_access_token().access_token).text