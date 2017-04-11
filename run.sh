cd "$(dirname "$0")"

# only do the setup if it has not already been done
if [ ! -d ~/.firecloud-tools ]; then
  	# add conditionals to only do if not existing
	pip install virtualenv

	virtualenv -q ~/.firecloud-tools/venv
	
	source ~/.firecloud-tools/venv/bin/activate
	
	pip install --upgrade pip
	pip install PyYAML
	pip install httplib2
	pip install google-api-python-client
	pip install gcs-oauth2-boto-plugin
	pip install retrying
	pip install firecloud
	pip install xlrd
	pip install --upgrade google-cloud-bigquery
	pip install .
fi

export PYTHONPATH=./:$PYTHONPATH
source ~/.firecloud-tools/venv/bin/activate

python $@