from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
import airflow

dag = DAG(
    dag_id="sparkingflow",
    default_args={
        "owner": "Natanael Amaro",
        "start_date": airflow.utils.dates.days_ago(0)
    },
    schedule_interval="@daily"
)

start = PythonOperator(
    task_id="start",
    python_callable=lambda: print("Jobs started"),
    dag=dag
)

python_job = SparkSubmitOperator(
    task_id='python_job',
    conn_id='spark-conn',
    application="jobs/source/helloworld.py",
    dag=dag
)
