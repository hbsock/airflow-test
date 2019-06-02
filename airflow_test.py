"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator

from datetime import datetime, timedelta

AWS_BUCKET_NAME = 'hanbin-test-poc-input'


default_args = {
    'owner': 'hsock',
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 30),
    'email': ['hbsock@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('hsock_tutorial', default_args=default_args, schedule_interval=timedelta(days=1))

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""

t3 = BashOperator(
    task_id='templated',
    bash_command=templated_command,
    params={'my_param': 'Parameter I passed in'},
    dag=dag)


s3_sensor = S3KeySensor(
    task_id='s3_key_sensor',
    bucket_key='some_folder/input/*',
    wildcard_match=True,
    bucket_name=AWS_BUCKET_NAME,
    timeout=10,
    poke_interval=60,
    dag=dag)

email_op = EmailOperator(
    task_id='send_email',
    subject='test email',
    to='hbsock@gmail.com',
    html_content='TEXT CONTENT',
    dag=dag)

s3_sensor >> t1 >> t3 >> email_op
