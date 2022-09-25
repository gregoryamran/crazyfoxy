from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import time 
 

dag = DAG(
          'test',           
           schedule_interval='1 * * * *',
           max_active_runs = 1 , 
           start_date=datetime(2017, 3, 20), catchup=False
      ) 

def print_hello():
    time.sleep(20)
    return 'Hello world from first Airflow DAG!'

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

hello_operator2 = PythonOperator(task_id='hello_task2', python_callable=print_hello, dag=dag)

hello_operator >> hello_operator2


# 
# 
# docker run -d -p 8080:8080 -v C:\Users\d_ros\Downloads\temp:/usr/local/airflow/dags puckel/docker-airflow webserver
# docker run -d -p 8080:8080 -v C:\Users\d_ros\Downloads\temp:/usr/local/airflow/dags apache/airflow 
# docker run -it -p 8080:8080 -v C:\Users\d_ros\Downloads\temp:/opt/airflow/dags hello_airflow webserver
# 
# dag_id = 'fake_dag_id'
# dag_runs = DagRun.find(dag_id=dag_id)
# for dag_run in dag_runs:
#       print(dag_run.state)
