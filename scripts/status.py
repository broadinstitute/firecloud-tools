from common import *

print "Here is the output from doing a call to the https://api.firecloud.org/register endpoint, "
print "showing you who you are authed as as well as your status in FireCloud - intended for"
print "debugging purposes."
print requests.get("https://api.firecloud.org/register", headers=firecloud_api._fiss_access_headers()).text