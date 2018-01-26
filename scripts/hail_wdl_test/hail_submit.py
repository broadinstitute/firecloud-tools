import argparse
import googleapiclient.discovery
import google.auth
from google.cloud import storage
import uuid
from cloudtools import start, submit, stop

class ClusterConfig(object):
    def __init__(self, name, balance=0.0):
        """Return a Customer object whose name is *name* and starting
        balance is *balance*."""
        self.name = name
        self.balance = balance

  
# functions 
def create_cluster(dataproc, project, region, cluster_name, 
                   master_machine_type, master_boot_disk_size, 
                   worker_num_instances, worker_machine_type, worker_boot_disk_size, worker_num_ssd, worker_preemptible):
    print "Creating cluster {} in project: {}".format(cluster_name, project)
    
    # https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.clusters#Cluster
    cluster_data = {
        'projectId': project,
        'clusterName': cluster_name,
        'config': {
            'gceClusterConfig': {
                "serviceAccountScopes": [
                  'https://www.googleapis.com/auth/userinfo.profile', 
                  'https://www.googleapis.com/auth/userinfo.email'
                ]
            },
            "masterConfig": {
                "numInstances": 1,
                "machineTypeUri": master_machine_type,
                "diskConfig": {
                   "bootDiskSizeGb": master_boot_disk_size
                }
            },
            "workerConfig": {
                "numInstances": worker_num_instances,
                "machineTypeUri": worker_machine_type,
                "diskConfig": {
                    "bootDiskSizeGb": worker_boot_disk_size,
                    "numLocalSsds": worker_num_ssd,
                },
                "isPreemptible": "true" if worker_preemptible else "false"
            },
            "softwareConfig": {
                "imageVersion": "1.1",
                "properties": {
                    "spark:spark.driver.extraJavaOptions":"-Xss4M",
                    "spark:spark.executor.extraJavaOptions":"-Xss4M",
                    "spark:spark.driver.memory":"45g",
                    "spark:spark.driver.maxResultSize": "30g",
                    "spark:spark.task.maxFailures":"20",
                    "spark:spark.kryoserializer.buffer.max":"1g",
                    "hdfs:dfs.replication":"1"
                }
            },
            "initializationActions": [
                {"executableFile": "gs://hail-common/hail-init.sh"},
                {"executableFile": "gs://hail-common/init_notebook.py"}
            ]
        }
    }
    result = dataproc.projects().regions().clusters().create(
        projectId=project,
        region=region,
        body=cluster_data).execute()
    return result
    
def wait_for_cluster_creation(dataproc, project_id, region, cluster_name):
    print('Waiting for cluster creation...')

    while True:
        result = dataproc.projects().regions().clusters().list(
            projectId=project_id,
            region=region).execute()
        cluster_list = result['clusters']
        cluster = [c
                   for c in cluster_list
                   if c['clusterName'] == cluster_name][0]
        if cluster['status']['state'] == 'ERROR':
            raise Exception(result['status']['details'])
        if cluster['status']['state'] == 'RUNNING':
            print("Cluster created.")
            break

def submit_pyspark_job(dataproc, spark_version, project, region,
                       cluster_name, bucket_name, hail_hash, hail_script_path, script_arguments):
     
    if not hail_hash:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket("hail-common")
        blob = bucket.blob( "latest-hash.txt" )
        # get the hash from this text file, removing any trailing newline
        hail_hash = blob.download_as_string().rstrip()
    
    hail_jar_file="hail-hail-is-master-all-spark{}-{}.jar".format(spark_version, hail_hash)
    hail_jar_path="gs://hail-common/{}".format(hail_jar_file)
    
    # upload the hail script to this dataproc staging bucket            
    upload_blob(bucket_name, hail_script_path, "script.py")
                
    """Submits the Pyspark job to the cluster, assuming `filename` has
    already been uploaded to `bucket_name`"""
    job_details = {
        'projectId': project,
        'job': {
            'placement': {
                'clusterName': cluster_name
            },
            # https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.jobs#pysparkjob
            'pysparkJob': {
                "mainPythonFileUri": "gs://{}/{}".format(bucket_name, "script.py"),
                "args": script_arguments,
                "pythonFileUris": [
				    "gs://hail-common/pyhail-hail-is-master-{}.zip".format(hail_hash),
				    "gs://{}/{}".format(bucket_name, "script.py")
                ],
                "jarFileUris": [
				    hail_jar_path
                ],
                # "fileUris": [
# 				    string
#                 ],
                # "archiveUris": [
# 				    string
#                 ],
                "properties": {
				    "spark.driver.extraClassPath":"./{}".format(hail_jar_file),
				    "spark.executor.extraClassPath":"./{}".format(hail_jar_file)
                },
                # "loggingConfig": {
# 				    object(LoggingConfig)
#                 },
            } 
        }
    }
    result = dataproc.projects().regions().jobs().submit(
        projectId=project,
        region=region,
        body=job_details).execute()
    job_id = result['reference']['jobId']
    print('Submitted job ID {}'.format(job_id))
    return job_id

def wait_for_job(dataproc, project, region, job_id):
    print('Waiting for job to finish...')
    while True:
        result = dataproc.projects().regions().jobs().get(
            projectId=project,
            region=region,
            jobId=job_id).execute()
        # Handle exceptions
        if result['status']['state'] == 'ERROR':
            raise Exception(result['status']['details'])
        elif result['status']['state'] == 'DONE':
            print('Job finished.')
            return result
                
def delete_cluster(dataproc, project, region, cluster):
    print('Tearing down cluster')
    result = dataproc.projects().regions().clusters().delete(
        projectId=project,
        region=region,
        clusterName=cluster).execute()
    return result

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name)) 

def list_clusters(dataproc, project, region):
	result = dataproc.projects().regions().clusters().list(
		projectId=project,
		region=region).execute()
	return result

def get_client():
	"""Builds a client to the dataproc API."""
	dataproc = googleapiclient.discovery.build('dataproc', 'v1')
	return dataproc

if __name__ == "__main__":
    main_parser = argparse.ArgumentParser(description='Submit and wait for a Hail job.')
    start.init_parser(main_parser)
    
    main_parser.add_argument('--files', required=False, type=str, help='Comma-separated list of files to add to the working directory of the Hail application.')
    main_parser.add_argument('--args', type=str, help='Quoted string of arguments to pass to the Hail script being submitted.')
    main_parser.add_argument('script', type=str)
    args = main_parser.parse_args()
    
    dataproc = get_client()

    try:
        cluster_name = "firecloud-hail-{}".format(uuid.uuid4())
        
        # cloudtools start 
        start.main(args)
        
        job_id = submit.main(args)
        job_result = wait_for_job(dataproc, args.project, args.region, job_id)
                
        # TODO: what do we need to do to handle successful jobs?
        print job_result          
    except Exception as e:
        print e
        raise
    finally:
        stop.main(args)