from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

#setting up the default args
default_args = {
    'owner': 'hugo',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#setting up the dag settings
dag = DAG(
    dag_id='Data_processing',
    default_args=default_args,
    description='Create a Database from a csv with Spark, Airflow and PSQL',
    schedule_interval='@monthly',
)

#setting up the job settings
job1 = SparkSubmitOperator(
    task_id='spark_job',
    conn_id="spark-conn" ,
    application="src/main/pyspark/spark_job.py",
    dag=dag,
)

job1
