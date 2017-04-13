# RETRY_LIMIT=5
# 
# until cat $1 | gsutil -m -o GSUtil:parallel_composite_upload_threshold=150M cp -L gsutil_uploads.log -c -I $2/; do
#   sleep 1
#   ((count++)) && ((count==$RETRY_LIMIT)) && break
# done

command="cat $1 | gsutil -m -o GSUtil:parallel_composite_upload_threshold=150M cp -L gsutil_uploads.log -c -I $2/"
eval $command

rc=$?
if [[ $rc != 0 ]]; 
then 
  echo "Error uploading data.  You can manually retry using the command: ${command}" 
  exit $rc
fi

# if [ "$count" = "$RETRY_LIMIT" ]; then
#   echo '\n\nERROR: Could not copy all the files to the workspace bucket.'
#   echo "You can manually retry using the command: ${command}"
#   exit 1
# fi