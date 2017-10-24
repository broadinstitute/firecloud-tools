workflow run_hail {
  File svcActKeyJson
  String dataprocProject
  File hailCommandFile
  String dataprocRegion
  
  call call_hail {
    input:
      svcActKeyJson=svcActKeyJson,
      dataprocProject=dataprocProject,
      hailCommandFile=hailCommandFile,
      dataprocRegion=dataprocRegion
  }
  
  output {
  }
  
  # TODO: verify clusters from all tasks are gone
}

task call_hail {
   File svcActKeyJson
   String dataprocProject
   File hailCommandFile
   String dataprocRegion
  
   command {
     gcloud auth activate-service-account --key-file=${svcActKeyJson} 
     export GOOGLE_APPLICATION_CREDENTIALS=${svcActKeyJson}
     
     # set project for all subsequent gcloud calls
     gcloud config set project ${dataprocProject}
     
     region=${dataprocRegion}
     # create Dataproc cluster
     cluster_name=firecloud-hail-$(date +%s)
     gcloud beta dataproc --region $region clusters create $cluster_name \
         --region $region \
         --master-machine-type n1-standard-8 \
         --master-boot-disk-size 100 \
         --num-workers 2 \
         --worker-machine-type n1-standard-8 \
         --worker-boot-disk-size 75 \
         --num-worker-local-ssds 0 \
         --num-preemptible-workers 2 \
         --image-version 1.1 \
         --properties "spark:spark.driver.extraJavaOptions=-Xss4M,spark:spark.executor.extraJavaOptions=-Xss4M,spark:spark.driver.memory=45g,spark:spark.driver.maxResultSize=30g,spark:spark.task.maxFailures=20,spark:spark.kryoserializer.buffer.max=1g,hdfs:dfs.replication=1" \
         --initialization-actions gs://hail-common/hail-init.sh,gs://hail-common/init_notebook.py
     
     # build url for hail jar
     HASH=`gsutil cat gs://hail-common/latest-hash.txt`

     JAR_FILE=hail-hail-is-master-all-spark2.0.2-$HASH.jar
     JAR=gs://hail-common/$JAR_FILE

     pyfiles="gs://hail-common/pyhail-hail-is-master-$HASH.zip,${hailCommandFile}"
    
     # submit this script to dataproc
     gcloud dataproc jobs submit pyspark \
       --cluster $cluster_name \
       --region $region \
       --files=$JAR \
       --py-files=$pyfiles \
       --properties="spark.driver.extraClassPath=./$JAR_FILE,spark.executor.extraClassPath=./$JAR_FILE" \
       ${hailCommandFile} \
       -- \
         --inputVds 'gs://fc-8624185a-6057-4997-9182-265963b53d69/data/1kg.vds' \
         --inputAnnot 'gs://fc-8624185a-6057-4997-9182-265963b53d69/data/1kg_annotations.txt' \
         --output 'gs://fc-8624185a-6057-4997-9182-265963b53d69/out/1kg_out.vds' \
         --qcResults 'gs://fc-8624185a-6057-4997-9182-265963b53d69/out/sampleqc.txt'    
     exit_code=$?
     
     
     
     # if we failed to submit, we want to still delete the cluster, but mark this
     # task as failed, so flag this as failed so we can exit with non-zero later
     failed=false

     if [ $exit_code -ne 0 ] ; then
       failed=true
     fi
               
     # shut down the cluster
     gcloud beta dataproc --region $region clusters delete -q $cluster_name --project=${dataprocProject}
     
     if [ $failed = true ] ; then
       echo "Failed to submit job..."
       exit 1
     else
       exit 0
     fi
   }
   runtime {
   	docker: "google/cloud-sdk:170.0.1-slim"
    memory: 1
    disks: "local-disk 1 HDD"
   }
   
   output {
     
   }
}