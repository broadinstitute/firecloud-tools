> [!WARNING]
> This repository is deprecated. Use [terra-tools](https://github.com/broadinstitute/terra-tools) instead.

# Tools for use with FireCloud
To run a given script using the run script:

  * `./run.sh scripts/directory/script_name.py <arguments>`

To run a giving script using Docker:

  * `docker run --rm -it -v "$HOME"/.config:/.config broadinstitute/firecloud-tools python /scripts/<script name.py> <arguments>`

## Prerequisites
* Install the Google Cloud SDK from https://cloud.google.com/sdk/downloads
* Set the Application Default Credentials (run `gcloud auth application-default login`)
* Python 2.7

When running without the run script or docker, check the packages that are pip
installed in either run.sh or the Dockerfile.
