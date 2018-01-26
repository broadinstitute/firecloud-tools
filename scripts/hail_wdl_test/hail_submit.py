import argparse
import googleapiclient.discovery
import google.auth
from google.cloud import storage
import uuid


# functions
def create_cluster(dataproc, project, region, cluster_name,
                   master_machine_type, master_boot_disk_size,
                   worker_num_instances, worker_machine_type,
                   worker_boot_disk_size, worker_num_ssd, worker_preemptible):
    print("Creating cluster {} in project: {}".format(cluster_name, project))

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


def submit_pyspark_job(dataproc, project, region,
                       cluster_name, bucket_name, hail_script_path, script_arguments):

    storage_client = storage.Client()
    bucket = storage_client.get_bucket("hail-common")
    blob = bucket.blob("latest-hash.txt" )
    # get the hash from this text file, removing any trailing newline
    hail_hash = blob.download_as_string().rstrip().decode()

    hail_jar_file="hail-hail-is-master-all-spark2.0.2-{}.jar".format(hail_hash)
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
#                     string
#                 ],
                # "archiveUris": [
#                     string
#                 ],
                "properties": {
                    "spark.driver.extraClassPath":"./{}".format(hail_jar_file),
                    "spark.executor.extraClassPath":"./{}".format(hail_jar_file)
                },
                # "loggingConfig": {
#                     object(LoggingConfig)
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
    parser = argparse.ArgumentParser(
        description='Run a hail script on dataproc')

    # the python hail script to execute on the dataproc cluster
    parser.add_argument('hailScript', nargs=1)

    # optional dataproc arguments
    parser.add_argument(
        '--dataprocMasterMachType', default='n1-standard-8',
        help='Machine type to use for the master machine, e.g. n1-standard-8.')
    parser.add_argument(
        '--dataprocMasterBootDiskSize', default=100, type=int,
        help='Size of the boot disk to use for the master machine in GB, e.g. 100')
    parser.add_argument(
        '--dataprocNumWorkers', default=2, type=int,
        help='Number of worker nodes, e.g. 2')
    parser.add_argument(
        '--dataprocWorkerMachType', default='n1-standard-8',
        help='Machine type to use for the worker nodes, e.g. n1-standard-8.')
    parser.add_argument(
        '--dataprocWorkerBootDiskSize', default=100, type=int,
        help='Size of the boot disk to use for the worker nodes in GB, e.g. 100')
    parser.add_argument(
        '--dataprocWorkerNumSSD', default=0, type=int,
        help='Number of SSD disks for use in the worker nodes, e.g. 2')
    parser.add_argument(
        '--dataprocWorkerPreemptible', required=False, action='store_true',
        default=False,
        help='Number of preemptible instances to use for worker nodes, e.g. 2')
    parser.add_argument(
        '--dataprocRegion', dest='dataprocRegion', required=False,
        default="us-central1",
        help='Optional region for use in choosing a region to create the cluster.  Defaults to us-central1.')
    parser.add_argument(
        '--project', required=False,
        help='Project to create the Dataproc cluster within. Defaults to the current project in the gcloud config.')

    # parse the args above as well user defined arguments (unknown below) that
    # will get passed to the hail script. the user defined arguments are
    # whatever arguments that are needed by the python hail script.

    args, script_args = parser.parse_known_args()

    dataproc = get_client()
    try:
        # get the current project from gcloud config
        project = args.project if args.project else google.auth.default()[1]
        cluster_name = "firecloud-hail-{}".format(uuid.uuid4())

        print("Creating cluster {} in project: {}".format(cluster_name, project))

        cluster_info = create_cluster(
            dataproc, project, args.dataprocRegion, cluster_name,
            args.dataprocMasterMachType, args.dataprocMasterBootDiskSize,
            args.dataprocNumWorkers, args.dataprocWorkerMachType,
            args.dataprocWorkerBootDiskSize, args.dataprocWorkerNumSSD,
            args.dataprocWorkerPreemptible)
        cluster_uuid = cluster_info["metadata"]["clusterUuid"]

        active_clusters = wait_for_cluster_creation(
            dataproc, project, args.dataprocRegion, cluster_name)
        clusters = list_clusters(dataproc, project, args.dataprocRegion)
        for cluster in clusters["clusters"]:
            if cluster["clusterUuid"] == cluster_uuid:
                cluster_staging_bucket = cluster["config"]["configBucket"]

                job_id = submit_pyspark_job(
                    dataproc, project, args.dataprocRegion,
                    cluster_name, cluster_staging_bucket, args.hailScript[0],
                    script_args)

                job_result = wait_for_job(
                    dataproc, project, args.dataprocRegion, job_id)

                # TODO: what do we need to do to handle successful jobs?
                print(job_result)
                break
    except Exception as e:
        print(e)
        raise
    finally:
        delete_cluster(dataproc, project, args.dataprocRegion, cluster_name)
