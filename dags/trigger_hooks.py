from custom_plugin import APIToPostgreSQLHook,PostgreSQLToCsv


def trigger_APIToPostgreSQLHook(postgres_conn_id, api_public_url, rows, **kwargs):
    date= kwargs['ti'].xcom_pull(key='date',task_ids='get_execution_date')
    APIToPostgreSQLHook().copy_table(postgres_conn_id, api_public_url, date, rows)
    
def trigger_PostgreSQLToCsv(postgres_conn_id, path, **kwargs):
    date= kwargs['ti'].xcom_pull(key='date',task_ids='get_execution_date')
    PostgreSQLToCsv().copy_table(postgres_conn_id, path, date)
