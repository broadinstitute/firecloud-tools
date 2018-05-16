#!/usr/bin/env python
from distutils.core import setup

setup(name='Cromwell Config setup',
      version='1.0',
      description='Setup an individual configuration file to run Cromwell on Google Cloud',
      author='Kate Voss',
      author_email='kvoss@broadinstitute.org',
      url='https://github.com/broadinstitute/firecloud-tools',
      packages=['google-api-python-client', 'google.auth', 'google.cloud', 'apiclient.discovery'],
     )