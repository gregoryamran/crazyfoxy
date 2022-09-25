from datetime import datetime, timedelta
from email.policy import default
from airflow import DAG
from airflow.utils import timezone 
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.xcom import XCom
from airflow.api.common.experimental.trigger_dag import trigger_dag 
from airflow.utils.trigger_rule import TriggerRule 
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

import time 
"""
{
    "data": {
        "analyze_command": "comformance.table1;comformance.table2;comformance.table3;comformance.table4"
    }
}
"""

dag = DAG(
          'housekeeping',           
           schedule_interval=None,
           max_active_runs = 1 , 
           start_date=datetime(2022, 9, 10), 
           catchup=False,
           default_args={ 'retries':0 , 'retry_delay': timedelta(minutes=0) , 'owner':'Titans' }
      ) 

def read_config(dag_run, *kwargs):
    print(dag_run.conf)
    # print(dag_run)
    # print(kwargs)
    return 'prepare_command'
   

def prepare_command(**context):
    dag_run_data = context['dag_run'].conf['data']
    print(dag_run_data['analyze_command'])
    ti = context['ti']
    ti.xcom_push(key="analyze_command", value=str(dag_run_data['analyze_command']).split(';')) 
    return 'execute_command'


def execute_command(**context):
    dag_run_data = context['dag_run'].conf['data']
    print(dag_run_data['analyze_command'])
    ti = context['ti']
    c = ti.xcom_pull( key="analyze_command_completed") 
    if c is None:
        c = [] 
    key_value = ti.xcom_pull(task_ids='prepare_command', key="analyze_command") 
    print(key_value[0])
    c.append(key_value[0])
    ti.xcom_push(key="analyze_command_completed", value=c) 
    print("c", c )
    print("key_value", key_value )
    key_value.remove(key_value[0]) 
    str_key_value = ";".join(key_value)
    print(str(str_key_value))
    ti.xcom_push(key="analyze_command", value=str_key_value )     
    # ti.xcom_push(key="analyze_command", value=str(dag_run_data['analyze_command']).split(';')) 
    dag_id = 'housekeeping'
    run_id = 'housekeeping_run_' + '_' + str(datetime.now().strftime("%Y%m%d%H%M%S"))
    conf = """{
        "data": {    "analyze_command": "comformance.table2"   }
    } """.replace("comformance.table2",str_key_value)
    execution_data = timezone.utcnow()
    if len(key_value) > 0 :             
        trigger_dag(
            run_id='housekeeping_' + str(datetime.now().strftime("%Y%m%d%H%M%S")) ,
            dag_id="housekeeping", 
            conf=conf
        )
        trigger_dag(
            run_id='hello_world_' + str(datetime.now().strftime("%Y%m%d%H%M%S")) ,
            dag_id="hello_world", 
            conf=conf
        )
        time.sleep(2)
        trigger_dag(
            run_id='hello_world_' + str(datetime.now().strftime("%Y%m%d%H%M%S")) ,
            dag_id="hello_world", 
            conf=conf
        )
        time.sleep(2)
        trigger_dag(
            run_id='hello_world_' + str(datetime.now().strftime("%Y%m%d%H%M%S")) ,
            dag_id="hello_world", 
            conf=conf
        )
    return 'final_status'


t1_bash = """
            echo 'Hello World'
        """

def final_status(**context):          
    print('final status')
  

def print_event_generator():
    time.sleep(2)
    return 'Hello world from first Airflow DAG!'

# TASK 
read_config = PythonOperator(
    task_id='read_config',
    provide_context=True,
    python_callable=read_config,
    dag=dag,
)

prepare_command = PythonOperator(
    task_id='prepare_command',
    provide_context=True,
    python_callable=prepare_command,
    dag=dag,
)

# [START howto_operator_bash]
t1 = BashOperator(
    task_id='t1',
    bash_command='echo "hello world"; ',
    dag=dag
)
# [END howto_operator_bash]

execute_command = PythonOperator(
    task_id='execute_command',
    provide_context=True,
    python_callable=execute_command,
    dag=dag,
)

final_status = PythonOperator(
    task_id='final_status',
    provide_context=True,
    python_callable=final_status,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

read_config >> t1 >> prepare_command >> execute_command >> final_status

# read_config >> prepare_command >> final_status




# generate_event = PythonOperator(task_id='generate_event', python_callable=print_event_generator, dag=dag)


# trigger = TriggerDagRunOperator(
#     task_id="trigger_id",
#     trigger_dag_id="hello_world", 
#     conf={"name": "Gregory "},
#     dag=dag,
# )

# generate_event >> trigger

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
