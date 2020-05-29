import requests
import sys
from subprocess import Popen, PIPE
import json


def run_subprocess(cmd, debug=False):
    p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
    stdout, stderr = p.communicate()
    
    stdout_str = stdout.decode("utf-8").strip()
    stderr_str = stderr.decode("utf-8").strip()

    if debug:
        print("StdOut: " + stdout_str)
        print("StdErr: " + stderr_str)

    if p.returncode != 0:  
        errorText = "ERROR: unable to call command: " + cmd + "\n\n" +  stdout_str + "\n\n" +  stderr_str    
        raise Exception(errorText)

    return stdout_str

    
def print_error(message):
    print(message, file=sys.stderr)

def get_user_account():
    current_account = run_subprocess("gcloud config list account --format 'value(core.account)'")
    if not "@" in current_account:
        safe_exit("ERROR: Unable to get current account, expected an email address but got: '" + current_account + "'")

    return current_account

def revert_to_user_account(account):
    run_subprocess("gcloud auth login " + current_account)

if __name__ == "__main__":
    
    drs_uri = None
    for arg in sys.argv:
        if "drs://" in arg:
            drs_uri = arg
    if not drs_uri:
        print_error("ERROR: Unable to get DRS URI from input command")
        exit(1)

    current_account = get_user_account()

    try:
        
        access_token = run_subprocess("gcloud auth print-access-token")
   
        martha_request = requests.post("https://us-central1-broad-dsde-prod.cloudfunctions.net/martha_v2",
                                       data = {'url': drs_uri},
                                       headers={"authorization": "Bearer " + access_token} )

        if martha_request.status_code != 200:
            raise Exception("Unable to get info on DRS URI: " + martha_request.text)
  
        martha_response = martha_request.json()

        martha_urls = martha_response["dos"]["data_object"]["urls"]
        if "gs://" not in str(martha_urls):
            raise Exception("ERROR: unable to find any google URLs in DRS response: " + str(martha_urls))

        gs_url = [url["url"] for url in martha_urls if "gs://" in url["url"]][0]

        martha_svc_key = martha_response["googleServiceAccount"]["data"]


        svc_key_path = "/tmp/svc_" + martha_svc_key["client_email"] + "_key.json"
        with open(svc_key_path, 'w') as svc_key_file:
            svc_key_file.write(json.dumps(martha_svc_key))
    

        run_subprocess("gcloud --verbosity=error auth activate-service-account  --key-file=" + svc_key_path)

        new_gsutil_command_args = ["gsutil"] 

        for arg in sys.argv[1:]:
            if "drs://" in arg:
                new_gsutil_command_args.append(gs_url)
            else:
                new_gsutil_command_args.append(arg)

        popen = Popen(new_gsutil_command_args, stdout=PIPE, universal_newlines=True)
        for stdout_line in iter(popen.stdout.readline, ""):
            print(stdout_line)
        popen.stdout.close()
    except Exception as e:
        print_error("ERROR using drsutil: \n" + str(e))
        exit(1)
    finally:
        revert_to_user_account (current_account)
    

    
