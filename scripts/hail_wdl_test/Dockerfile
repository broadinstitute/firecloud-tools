FROM google/cloud-sdk:170.0.1-slim

COPY hail_submit.py /hail_submit.py

# TODO: switch to actual repo once PR is merged
RUN pip install git+https://github.com/abaumann/cloudtools.git@ab_client_lib
RUN pip install --upgrade google-api-python-client
RUN pip install google-cloud-storage google-auth-httplib2

ENV PYTHONPATH=$PYTHONPATH:/