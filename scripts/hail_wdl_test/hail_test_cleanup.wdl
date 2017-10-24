workflow run_hail {
  File svcActKeyJson
  String dataprocProject
  File hailCommandFile
  String dataprocRegion
  
  call vcf_to_vds {} 
  
  Map[String, String] hail_input_map = ("inputVds", vcf_to_vds.output_vds)
  
  
  call call_hail {
    input:
      svcActKeyJson=svcActKeyJson,
      dataprocProject=dataprocProject,
      hailCommandFile=hailCommandFile,
      dataprocRegion=dataprocRegion
      inputMap=hail_input_map 
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
   String output
   String qcResults
  
   command {
     # for now until service accounts are natively available, we need to auth as the svc account
     gcloud auth activate-service-account --key-file=${svcActKeyJson} 
     export GOOGLE_APPLICATION_CREDENTIALS=${svcActKeyJson}
     
     # set project for all subsequent gcloud calls
     gcloud config set project ${dataprocProject}
     
     python ${hailSubmitFile} \
         --dataprocMasterMachType n1-standard-8 \
         --dataprocMasterBootDiskSize 100 \
         --dataprocNumWorkers 2 \
         --dataprocWorkerMachType n1-standard-8 \
         --dataprocWorkerBootDiskSize 100 \
         --dataprocWorkerNumSSD 2 \
         --dataprocWorkerNumPreemptible 2  
         ${hailCommandFile} \
             --inputVds ${inputVds} \
             --inputAnnot ${inputAnnot}
             --output ${output} \
             --qcResults ${qcResults}
   }
   runtime {
   	docker: "google/cloud-sdk:170.0.1-slim"
    memory: 1
    disks: "local-disk 1 HDD"
   }
   
   output {
     
   }
}