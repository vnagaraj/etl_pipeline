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

input_file = '/home/ubuntu/efs/etl_pipeline/airflow/pipelines'


with open(input_file,'r') as f:
    for line in f:
        dagid = line.strip().split(',')[0]
        #yaml = line.strip().split(',')[1]
        dag = DAG(
            dagid, default_args=default_args, schedule_interval="@once")
        globals()[dagid] = dag
        readMessageFromQueue = BashOperator(
              task_id='readMessageFromQueue',
            bash_command= "cd ~/efs/etl_pipeline/ && mvn exec:java -Dexec.mainClass=etl.ReadMessageFromQueue -Dexec.args=" +dagid,
            dag=dag)
        readRedis = BashOperator(
              task_id='readRedis',
            bash_command= "cd ~/efs/etl_pipeline/ && mvn exec:java -Dexec.mainClass=etl.ReadRedis -Dexec.args=" +dagid,
            dag=dag)
        flinkTransform = BashOperator(
              task_id='flinkTransform',
            bash_command= "ssh ubuntu@flinkmaster  " + "'/usr/local/flink/bin/flink run  -c etl.FlinkTransform /home/ubuntu/efs/etl_pipeline/target/etl-0.1.jar -d "+dagid+"'", dag=dag)
        readRedis.set_upstream(readMessageFromQueue)
        flinkTransform.set_upstream(readRedis)
f.close()
