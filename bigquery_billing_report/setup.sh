#apt-get update
#apt-get install -y -qq --no-install-recommends python python-pip && apt-get -yq autoremove && apt-get -yq clean

pip install virtualenv

virtualenv -q ~/.workbench_pm/venv

source ~/.workbench_pm/venv/bin/activate

pip install httplib2
pip install google-api-python-client
pip install retrying
pip install firecloud

python pm_script.py