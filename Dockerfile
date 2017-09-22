FROM google/cloud-sdk:170.0.1-slim

RUN pip install virtualenv
RUN virtualenv -q ~/.firecloud-tools/venv
RUN /bin/bash -c "source ~/.firecloud-tools/venv/bin/activate"
	
RUN pip install --upgrade pip

RUN pip install PyYAML google-auth-httplib2 google-api-python-client gcs-oauth2-boto-plugin \ 
retrying firecloud xlrd google-cloud-bigquery google-cloud-logging pandas

RUN export PYTHONPATH=./:$PYTHONPATH
CMD ["/bin/bash"]
