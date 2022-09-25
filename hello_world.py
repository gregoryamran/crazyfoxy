from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import time 

def print_hello():
    time.sleep(50)
    return ('Hello world from first Airflow DAG!')

def _take_data(**context):
    DAGA_data = context["dag_run"].conf


dag = DAG('hello_world', description='Hello World DAG',
          schedule_interval=None,
          max_active_runs = 1 , 
          start_date=datetime(2017, 3, 20), catchup=False)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

data_from_DAGA = PythonOperator(
    task_id='data_from_DAGA',
    provide_context=True,
    python_callable=_take_data,
    dag=dag,
)



hello_operator >> data_from_DAGA


# 
# 
# docker run -d -p 8080:8080 -v C:\Users\d_ros\Downloads\temp:/usr/local/airflow/dags puckel/docker-airflow webserver
# docker run -d -p 8080:8080 -v C:\Users\d_ros\Downloads\temp:/usr/local/airflow/dags apache/airflow 
# docker run -it -p 8080:8080 -v C:\Users\d_ros\Downloads\temp:/opt/airflow/dags hello_airflow webserver
# 