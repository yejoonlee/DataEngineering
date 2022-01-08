# 다음과 같이 pipeline을 정의한다.
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'start_date': datetime(2021, 1, 1),
}

with DAG(dag_id='spark-example',
         schedule_interval='@daily',
         default_args=default_args,
         tags=['spark'],
         catchup=False) as dag:
    # 아래와 같이 SparkOperator를 활용하여 바로 Spark 작업을 정의하고 실행할수도 있지만
    # Airflow에서는 무거운 작업을 돌리지 않는게 좋다
    # sql_job = SparkSqlOperator(sql="SELECT * FROM bar", master="local", task_id="sql_job")

    # 따라서 Spark를 돌리는 파일을 미리 작성해두고 SparkSubmitOperator를 통해
    # Airflow에서는 Submit만 수행할 수 있도록 하는 것이 좋다.
    submit_job = SparkSubmitOperator(
        application="/Users/yeznable/Documents/GitHub/Data_Processing/Spark/count_trips_sql.py", task_id="submit_job",
        conn_id="spark_local"
    )