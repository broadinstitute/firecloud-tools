workflow run_hail {
  File svcActKeyJson
  String dataprocProject
  File hailCommandFile
  File hailSubmitFile
  String dataprocRegion
  
  String inputVds
  String inputAnnot
   
  File outputFile
  File qcResults
  
  call call_hail {
    input:
      svcActKeyJson=svcActKeyJson,
      dataprocProject=dataprocProject,
      hailCommandFile=hailCommandFile,
      hailSubmitFile=hailSubmitFile,
      dataprocRegion=dataprocRegion,
      inputVds=inputVds,
      inputAnnot=inputAnnot,
      outputFile=outputFile,
      qcResults=qcResults
  }
  
  output {
  }
  
  # TODO: verify clusters from all tasks are gone
}

task call_hail {
   File svcActKeyJson
   String dataprocProject
   File hailCommandFile
   # TODO remove this and get from github
   File hailSubmitFile
   String dataprocRegion
   
   String inputVds
   String inputAnnot
   
   File outputFile
   File qcResults
  
   command <<<
     # for now until service accounts are natively available, we need to auth as the svc account
     gcloud auth activate-service-account --key-file=${svcActKeyJson} 
     export GOOGLE_APPLICATION_CREDENTIALS=${svcActKeyJson}
     
     # set project for all subsequent gcloud calls
     gcloud config set project ${dataprocProject}
     
     # TODO outputs are Files, document why (call caching)
     # TODO optional delocalization in submit script
     # TODO tell chris about this bug https://github.com/broadinstitute/firecloud-tools/blob/ab_hail_wdl/scripts/hail_wdl_test/hail_test_cleanup.wdl#L59
     
     # TODO move any user defined params to task inputs
     python <<CODE 
     from hail_submit import *
     cluster_name = "firecloud-hail-{}".format(uuid.uuid4())
        
     print "Creating cluster {} in project: {}".format(cluster_name, "${dataprocProject}")
          
     try:
         dataproc_region = "${dataprocRegion}"
         dataproc_project = "${dataprocProject}"
         dataproc = get_client()    
         cluster_info = create_cluster(dataproc, dataproc_project, dataproc_region, cluster_name,
                                       "n1-standard-8", 100, 2,
                                       "n1-standard-8", 75, 0,
                                       False)
         cluster_uuid = cluster_info["metadata"]["clusterUuid"]
         
         
         active_clusters = wait_for_cluster_creation(dataproc, dataproc_project, dataproc_region, cluster_name)
         clusters = list_clusters(dataproc, project, args.dataproc_region)
         for cluster in clusters["clusters"]:
             if cluster["clusterUuid"] == cluster_uuid:
                 cluster_staging_bucket = cluster["config"]["configBucket"]
                 
                 script_args = """--inputVds ${inputVds} 
                                  --inputAnnot ${inputAnnot} \ 
                                  --output gs://{}/1kg_out.vds \
                                  --qcResults gs://{}/sampleqc.txt""".format(cluster_staging_bucket, cluster_staging_bucket).split(" ")
                 print script_args

                            
                 job_id = submit_pyspark_job(dataproc, dataproc_project, dataproc_region,
                                             cluster_name, cluster_staging_bucket, "${hailCommandFile}", script_args)
            
                 job_result = wait_for_job(dataproc, dataproc_project, dataproc_region, job_id)
                
                 print job_result          
                 break
     except Exception as e:
         print e
         raise
     finally:
         delete_cluster(dataproc, project, args.dataproc_region, cluster_name)
     CODE
   >>>

   runtime {
   	docker: "hail-submit"
    memory: 1
    disks: "local-disk 1 HDD"
   }
   
   output {
   }
}