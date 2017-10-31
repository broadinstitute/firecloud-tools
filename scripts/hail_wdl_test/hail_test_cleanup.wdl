workflow run_hail {
  File svcActKeyJson
  String dataprocProject
  String dataprocRegion

  File hailCommandFile
  String inputVds
  String inputAnnot
   
  File outputVdsFileName
  File qcResultsFileName
  
  call call_hail {
    input:
      svcActKeyJson=svcActKeyJson,
      dataprocProject=dataprocProject,
      hailCommandFile=hailCommandFile,
      dataprocRegion=dataprocRegion,
      inputVds=inputVds,
      inputAnnot=inputAnnot,
      outputVdsFileName=outputVdsFileName,
      qcResultsFileName=qcResultsFileName
  }
  
  output {
  }
  
  # TODO: verify clusters from all tasks are gone
}

task call_hail {
   File svcActKeyJson
   String dataprocProject
   # NOTE: right now it is localizing the python hail command file from a bucket - this could
   #       instead localize from e.g. github, however if we do the localization here using wget,
   #       then call caching won't know if the hail command file changed.  
   File hailCommandFile
   String dataprocRegion
   
   String inputVds
   String inputAnnot
   
   String outputVdsFileName
   String qcResultsFileName

  
   command <<<
     # for now until service accounts are natively available, we need to auth as the svc account
     gcloud auth activate-service-account --key-file=${svcActKeyJson} 
     export GOOGLE_APPLICATION_CREDENTIALS=${svcActKeyJson}
     
     # set project for all subsequent gcloud calls
     gcloud config set project ${dataprocProject}
     
     ##########################################################################################################
     # NOTE: Unfortunately in order to support call caching we will need to localize the files to this
     #       VM so that Cromwell knows about the files and adds them to the call caching.  To do this, 
     #       the Hail jobs will write to the Dataproc staging bucket and then get copied to this VM and
     #       referenced in the task output block.
     ##########################################################################################################
     
     python <<CODE 
     from hail_submit import *
     import os
     cluster_name = "firecloud-hail-{}".format(uuid.uuid4())
     dataproc_region = "${dataprocRegion}"
     dataproc_project = "${dataprocProject}"
     
     try:
         dataproc = get_client()
         # update cluster specs here as appropriate for the hail command 
         cluster_info = create_cluster(dataproc, dataproc_project, dataproc_region, cluster_name,
                                       "n1-standard-8", 100, 2,
                                       "n1-standard-8", 75, 0,
                                       False)
         cluster_uuid = cluster_info["metadata"]["clusterUuid"]
         
         active_clusters = wait_for_cluster_creation(dataproc, dataproc_project, dataproc_region, cluster_name)
         # list the clusters in the project and look for this specific cluster
         clusters = list_clusters(dataproc, dataproc_project, dataproc_region)
         for cluster in clusters["clusters"]:
             if cluster["clusterUuid"] == cluster_uuid:
                 cluster_staging_bucket = cluster["config"]["configBucket"]
                 
                 # build argument array - update this with your python hail command arguments.
                 # see note above about call caching - outputs go to the staging bucket
                 script_args = [ "--inputVds","${inputVds}",
                                 "--annot", "${inputAnnot}",
                                 "--outputVds", "gs://"+cluster_staging_bucket+"/"+"${outputVdsFileName}",
                                 "--qcResults", "gs://"+cluster_staging_bucket+"/"+"${qcResultsFileName}"
                               ]
                 job_id = submit_pyspark_job(dataproc, dataproc_project, dataproc_region,
                                             cluster_name, cluster_staging_bucket, "${hailCommandFile}", script_args)
                                             
                 job_result = wait_for_job(dataproc, dataproc_project, dataproc_region, job_id)
                
                 print job_result
                 break
         
         # see note above about call caching - copy files from staging bucket onto the local disk
         print os.popen("gsutil cp -r gs://{}/{} .".format(cluster_staging_bucket, "${qcResultsFileName}")).read()
         print os.popen("gsutil cp -r gs://{}/{} .".format(cluster_staging_bucket, "${outputVdsFileName}")).read()
     except Exception as e:
         print e
         raise
     finally:
         delete_cluster(dataproc, dataproc_project, dataproc_region, cluster_name)
     CODE
     
     # NOTE: delocalization does not work here to get a specific directory, only the contents of a directory via
     #       globs.  since vds files are folders and we need to retain the folder itself, we instead tar the
     #       directory and will need to untar it in downstream tasks in order to make use of it.
     tar -cvf ${outputVdsFileName}.tar ${outputVdsFileName}
   >>>
   
   runtime {
   	docker: "broadinstitute/firecloud-tools:hail_wdl"
    memory: 1
    disks: "local-disk 1 HDD"
   }
   
   # see note above about call caching - reference the files that were localized to this VM
   output {
     File outputVdsTar = "${outputVdsFileName}.tar"
     File qcResults = "${qcResultsFileName}"
   }
}