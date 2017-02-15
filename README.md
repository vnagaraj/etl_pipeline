# Project Title

Deploying ETL Pipelines

## Getting Started

Clone the following repo, https://github.com/vnagaraj/etl_pipeline.git in a shared storage.
if you are using a distributed envt
( make sure /etl_pipeline is in shared location for airflow master, airflow worker).
For remote flink master, make sure the /efs/etl_pipeline is the repo location.


## Prerequisites

###Software

1. Amazon S3 buckets ( for input and output).
2. Amazon SQS queue ( to be configured to receive event noticiations when file is uploaded to S3).
3. Airflow installation (preferably in distributed mode, 1 airflow master and 2 airflow workers).
   Please refer to the following ( http://site.clairvoyantsoft.com/installing-and-configuring-apache-airflow/).
4. Redis installation in a single node ( preferably a separate node ).
   Please refer to the following ( https://github.com/InsightDataScience/data-engineering-ecosystem/wiki/Redis).
5. Flink installation in cluster mode ( 1 flink master with 3 flink workers).
   Please refer to the following (https://ci.apache.org/projects/flink/flink-docs-release-0.8/cluster_setup.html).

###Configure Setting

1. Specify ~/.ssh/config in all nodes with Amazon pem key and the flinkMaster location. <br>
eg)<br>

    Host flinkmaster<br>
        HostName <public DNS of flinkMaster><br>
        User ubuntu<br>
        IdentityFile ~/.ssh/<pem key><br>
    <br>
    Host masternode<br>
        HostName <public DNS of airflowmaster><br>
        User ubuntu<br>
        IdentityFile ~/.ssh/<pem key><br>
    <br>
    Host slavenode1<br>
        HostName <public DNS of airflow worker1><br>
        User ubuntu<br>
        IdentityFile ~/.ssh/<pem key><br>
    <br>
    Host slavenode2<br>
        HostName <public DNS of airflow worker2><br>
        User ubuntu<br>
        IdentityFile ~/.ssh/<pem key><br>

2. Specify the property values in the config.properties ( ~/config.properties).

3. Compile and build jars for running the project<br>
eg) ubuntu@node:/etl_pipeline$ mvn clean<br>
    ubuntu@node:/etl_pipeline$ mvn package -Dmaven.test.skip=true

## Steps

### Simulate Data

1. Generate files to upload to S3 input bucket. <br>
ubuntu@node:/etl_pipeline$ python  ./scripts/file_generator.py <count> <br>
eg)ubuntu@node:/etl_pipeline$ python ./scripts/file_generator.py 30 <br>
   This command generates 30 files in range(input_1.txt - input_30.txt) in location etl_pipeline/files.
2. Generate userconfig info. <br>
ubuntu@node:/etl_pipeline$ python  ./scripts/yaml_generator.py <count> <br>
eg)ubuntu@node:/etl_pipeline$ python ./scripts/yaml_generator.py 30 <br>
   This command generates 30 files in range(user1.yaml - user30.yaml) in location 
   etl_pipeline/userconfig. 
3. Generate pipelines. <br>
ubuntu@ip-172-31-0-13:~/efs/etl_pipeline$ python ./scripts/pipeline_gen.py <count><br>
eg)ubuntu@node:/etl_pipeline$ python ./scripts/yaml_generator.py 35<br>
   This command updates the file etl_pipeline/airflow/pipelines with values ranging from et1-etl35,separated by newline.
   By rule of thumb, no of messages in SQS queue >= no of file uploads in S3 bucket and pipeline phase starts
   reading messages from the queue, so preferable to have pipelines generated for every message in queue to not
   miss any userInfo.

### Preprocessing stage

1. Upload files into S3 bucket. <br>
ubuntu@node:/etl_pipeline$ mvn exec:java -Dexec.mainClass=etl.UploadFiles -Dexec.args=files <br>
 This command uploads all the files in location etl_pipeline/files/ to S3 bucket.
2. Store fileName with userInfo in Redis key/value store. <br>
ubuntu@node:/etl_pipeline$ mvn exec:java -Dexec.mainClass=etl.StoreRedis -Dexec.args=userconfig <br>
 This command stores the filename as key and yaml as value for each user#.yaml file. 

### Running the pipelines

1. Specify the following configuration for airflow master and worker nodes
(~/airflow/airflow.cfg)
dags_folder = /etl_pipeline/airflow.

2. Load the dags for airflow. <br>
ubuntu@airflownode:~/etl_pipeline$ python airflow/airflowDag.py

3. Launch airflow worker. <br>
ubuntu@airflowworkernode:airflow worker

4. Launch the scheduler from airflow master.<br>
ubuntu@airflowmasternode:airflow scheduler

5. Track the state of the pipelines.<br>
http://<airflowmaster public DNS>:5555/admin/

6. Track the state of the flink execution.<br>
http://<flinkMaster>:8081/#/overview


##Testing

ubuntu@airflowmasternode:~/efs/etl_pipeline$ ./airflow/airflowtest.sh <br>
Verified all the stages of a single pipeline.

