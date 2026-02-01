from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from datetime import datetime, timedelta


project_path = "cd /home/developer/projects/spark-course-python/spark_course_python/spark_project/"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=20) }

with DAG(
    dag_id = 'spark_kafka_alert_stream',
    default_args=default_args,
    description= 'Runs ex4 -> ex5 -> ex6 -> ex7 sequentially',
    schedule_interval='@once',
    catchup=False,
    tags=['spark','pipeline']
    ) as dag:

    task_ex4 = SSHOperator(
        task_id='run_data_generator',
        ssh_conn_id='ssh_dev_env',
       command=f"""
        {project_path} && \
        spark-submit \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0\
        ex4_data_generator.py
        """,
        timeout=None
)

    task_ex5 = SSHOperator(
        task_id='run_data_enrichment',
        ssh_conn_id='ssh_dev_env',
        command=f"""
        {project_path} && \
        spark-submit \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0\
        ex5_data_enrichment.py
        """,
        timeout=None
    )

    task_ex6 = SSHOperator(
        task_id='run_alert_detection',
        ssh_conn_id='ssh_dev_env',
        command=f"""
        {project_path} && \
        spark-submit \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0\
        ex6_alert_detection.py
        """,
      timeout=None

    )

    task_ex7 = SSHOperator(
        task_id='run_alert_counter',
        ssh_conn_id='ssh_dev_env',
        command=f"""
        {project_path} && \
        spark-submit \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0\
        ex7_alert_counter.py
        """,
     timeout=None

    )

    task_ex4 >> task_ex5 >> task_ex6 >> task_ex7