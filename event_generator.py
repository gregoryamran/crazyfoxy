from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import time 
 

dag = DAG(
          'event_generator',           
           schedule_interval='1 * * * *',
           max_active_runs = 1 , 
           start_date=datetime(2017, 3, 20), catchup=False
      ) 

def print_event_generator():
    time.sleep(2)
    return 'Hello world from first Airflow DAG!'

generate_event = PythonOperator(task_id='generate_event', python_callable=print_event_generator, dag=dag)


trigger = TriggerDagRunOperator(
    task_id="trigger_id",
    trigger_dag_id="hello_world", 
    conf={"name": "Gregory "},
    dag=dag,
)

generate_event >> trigger

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
