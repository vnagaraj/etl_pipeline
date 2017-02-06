from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os


default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date': datetime.now(),
  'retries': 1,
  'retry_delay': timedelta(minutes=1),
}

input_file = '/Users/vivekanandganapathynagarajan/Documents/Insight/etl_pipeline/airflow/dags'


with open(input_file,'r') as f:
    for line in f:
        dagid = line.strip().split(',')[0]
        yaml = line.strip().split(',')[1]
        dag = DAG(
            dagid, default_args=default_args, schedule_interval="@once")
        globals()[dagid] = dag
        uploadFile= BashOperator(
            task_id='uploadS3',
            bash_command= "cd ~/Documents/Insight/etl_pipeline/ && mvn exec:java -Dexec.mainClass=etl.UploadFile -Dexec.args=" + yaml,
            dag=dag)
        readSQS = BashOperator(
              task_id='readSQS',
            bash_command= "cd ~/Documents/Insight/etl_pipeline/ && mvn exec:java -Dexec.mainClass=etl.ReadFromQueue -Dexec.args=" + yaml,
            dag=dag)
        readSQS.set_upstream(uploadFile)
        setupJar = BashOperator(
              task_id='setupJar',
            bash_command= "scp ~/Documents/Insight/etl_pipeline/target/etl-0.1.jar ubuntu@masternode:/home/ubuntu",
            dag=dag)
        setupJar.set_upstream(readSQS)
        copyYaml = BashOperator(
          task_id='uConfig' + dagid,
        bash_command= "scp ~/Documents/Insight/etl/log4j.properties ubuntu@masternode:/home/ubuntu && scp ~/Documents/Insight/etl/userconfig/" + yaml + " ubuntu@masternode:/home/ubuntu",
        dag=dag)
        copyYaml.set_upstream(readSQS)
        flinkTransform = BashOperator(
          task_id='flinkTransform',
        bash_command= "ssh ubuntu@masternode " + "'/usr/local/flink/bin/flink run  -c etl.FlinkTransform ~/etl-0.1.jar -u " + yaml+"'",
        dag=dag)
        flinkTransform.set_upstream(copyYaml)
f.close()
