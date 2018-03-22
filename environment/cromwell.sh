$ ./cromwell.sh local hello.wdl hello.inputs
$ # runs: java -jar cromwell.jar run hello.wdl -i hello.inputs
$ ./cromwell.sh google hello.wdl hello.inputs
$ # runs: java -Dconfig.file=google.conf -jar cromwell.jar run hello.wdl -i hello.inputs

$1 and $2 to access argv

