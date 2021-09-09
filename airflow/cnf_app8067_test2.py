"""
Example usage of the TriggerDagRunOperator. This example holds 2 DAGs:
1. 1st DAG (example_trigger_controller_dag) holds a TriggerDagRunOperator, which will trigger the 2nd DAG
2. 2nd DAG (example_trigger_target_dag) which will be triggered by the TriggerDagRunOperator in the 1st DAG
    
    { "message":"test message" }
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
 
my_var = Variable.get("SRC_CNF")
value_1 = [1, 2, 3]
value_2 = {'a': 'b'}

def run_this_func(dag_run, **kwargs):
    print(my_var)
    for item in kwargs:
        print(item , ' ' , kwargs[item])
        
    print(kwargs['task_instance'])
    task_instance = kwargs['task_instance']
    kwargs['ti'].xcom_push(key='value from pusher 1', value=value_1)
    kwargs['ti'].xcom_push(key='value from pusher 2', value=value_2)
    # kwargs['ti'].xcom_push(key='kwargs_key', value=kwargs)
    #task_instance.xcom_push(key="t1", value=run_this_next)
    print(dag_run.conf)
    print(f"Remotely received value of {dag_run.conf['message']} for key=message")


def extract_event_content(**context):
    print('this next func  ') 
    for item in context:
        print(item , ' ' , context[item])
    
    x = context['ti'].xcom_pull(key='value from pusher 1', task_ids="run_this" )
    print(x)
    x = context['ti'].xcom_pull(task_ids="run_this_func" )
    print( "task_ids=run_this ----- " , x)
    x = context['ti'].xcom_pull(key='kwargs_key' )
    print( "context ----- " , x)



with DAG(
    dag_id="cnf_app8067_test2",   
    start_date=days_ago(2),     
    schedule_interval=None,  
    catchup=False,   
    tags=['app8067'],    
) as dag:

    run_this = PythonOperator(task_id="run_this", python_callable=run_this_func , provide_context=True, dag=dag)
    run_this_next = PythonOperator(task_id="run_this_next", python_callable=extract_event_content , provide_context=True, dag=dag)
    
    bash_task = BashOperator(
        task_id="bash_task",
        bash_command='echo "Here is the message: $message"',
        env={'message': '{{ dag_run.conf["message"] if dag_run else "" }}'},
    )

run_this >> run_this_next >> bash_task
