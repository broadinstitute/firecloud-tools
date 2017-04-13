#apt-get update
#apt-get install -y -qq --no-install-recommends python python-pip && apt-get -yq autoremove && apt-get -yq clean

cd "$(dirname "$0")"

# only do the setup if it has not already been done
if [ ! -d ~/.dsdespecops ]; then
  	# add conditionals to only do if not existing
	pip install virtualenv

	virtualenv -q ~/.dsdespecops/venv
	
	source ~/.dsdespecops/venv/bin/activate
	
	pip install --upgrade pip
	pip install PyYAML
	pip install httplib2
	pip install google-api-python-client
	pip install gcs-oauth2-boto-plugin
	pip install retrying
	pip install firecloud
	pip install xlrd
	pip install --upgrade google-cloud-bigquery
fi

source ~/.dsdespecops/venv/bin/activate

python $@