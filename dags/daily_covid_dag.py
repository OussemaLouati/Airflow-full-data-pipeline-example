from airflow import DAG
from datetime import datetime, timedelta
from custom_plugin import CovidDailyDataSensor
from airflow.operators.email_operator import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from trigger_hooks import trigger_PostgreSQLToCsv, trigger_APIToPostgreSQLHook
import re

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 5,1),
    'email': [Variable.get("TARGET_EMAIL")],
	'email_on_failure': True,
	'email_on_retry': False,
	'retry_delay' : timedelta(seconds=300),
	'retries': 3,
}


def change_date_format(dt):
    return re.sub(r'(\d{4})-(\d{1,2})-(\d{1,2})', '\\3-\\2-\\1', dt)

def transform_data(exec_date, **kwargs):
    date=change_date_format(exec_date)
    print(f"Ingesting data for date: {date}")
    ti = kwargs['ti']
    ti.xcom_push(key="date", value=date)




with DAG('daily_cases_per_country_workflow4',
         default_args=default_args,
         schedule_interval='@daily', 
         template_searchpath=['/usr/local/airflow/sql_files'], 
         catchup=True) as dag:
    
    t0 = PythonOperator(
        task_id = 'get_execution_date',
        python_callable = transform_data,  
        op_args=["{{ execution_date | ds }} "],      
    )

    t1 = CovidDailyDataSensor(
        api_public_url = Variable.get("API_PUBLIC_URL"),
        task_id = 'data_check_sensor',
        poke_interval = 1800,
        timeout = 86400,
        )


    t2 = PostgresOperator(task_id='create_postgres_table', 
                          postgres_conn_id=Variable.get("POSTGRES_CONN_ID"), 
                          sql="create_table.sql",

                          )

    t3 = PythonOperator(
        task_id = 'insert_into_table',
        python_callable = trigger_APIToPostgreSQLHook,
        op_kwargs={"postgres_conn_id":Variable.get("POSTGRES_CONN_ID"), 
                   "api_public_url":Variable.get("API_PUBLIC_URL"),
                   "rows":['provinceState','countryRegion',"confirmed","deaths","recovered",'lastUpdate']
                   },
        provide_context=True
    )

    t4 = PythonOperator(
        task_id = 'select_from_table',
        python_callable = trigger_PostgreSQLToCsv,
        op_kwargs={"postgres_conn_id":Variable.get("POSTGRES_CONN_ID"), 
                   "path":Variable.get("CSV_FILE_PATH"),
                   },
        provide_context=True
    )
    

    t5 = EmailOperator(task_id='send_email',
        to=Variable.get("TARGET_EMAIL"),
        subject= "{{ ti.xcom_pull(key='date',task_ids='get_execution_date') }}" + " Daily report generated",
        html_content=""" <h1>Congratulations! Your reports is ready.</h1> """,
        files=[Variable.get("CSV_FILE_PATH") + "/cases_per_country" + "{{ ti.xcom_pull(key='date',task_ids='get_execution_date') }}" + ".csv"],
        )


    t0 >> t1 >> t2 >> t3 >> t4 >> t5
    
