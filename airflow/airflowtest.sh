#remove airflow logs in worker1
ssh ubuntu@slavenode1 'rm -rf ~/airflow/logs/e*'
#remove airflow logs in worker2
echo "Cleaned up airlfow logs in airflow worker-1"
ssh ubuntu@slavenode2 'rm -rf ~/airflow/logs/e*'
echo "Cleaned up airlfow logs in airflow worker-2"
#remove airflow tmp logs in airflow masternode
rm -rf ~/efs/etl_pipeline/tmp/e*
echo "Cleaned up airlfow logs in airflow master"
#delete files from input/output bucket in S3
mvn exec:java -Dexec.mainClass=etl.CleanupFiles
# generate one file
python scripts/file_generator.py
#Upload file to S3
mvn exec:java -Dexec.mainClass=etl.UploadFiles -Dexec.args=files
#Yaml generator
python scripts/yaml_generator.py
#pipeline generator
python scripts/pipeline_gen.py
#Store Redis
mvn exec:java -Dexec.mainClass=etl.StoreRedis -Dexec.args=userconfig
#airflow test tasks
airflow test etl1 readMessageFromQueue  02-14-2017
airflow test etl1 readRedis 02-14-2017
airflow test etl1 flinkTransform 02-14-2017
