set -e
cd "$(dirname "$0")"
# only do the setup if it has not already been done
if [ ! -d .firecloud-tools ]; then
  	# add conditionals to only do if not existing
	pip install virtualenv

	virtualenv -q .firecloud-tools/venv
	
    source .firecloud-tools/venv/bin/activate
    ./install.sh
fi

source .firecloud-tools/venv/bin/activate
export PYTHONPATH=./:scripts:$PYTHONPATH
python -u $@
