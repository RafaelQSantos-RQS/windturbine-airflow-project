import os
import json
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'depends_on_past' : False,
    'email' : ['rafael.qsantos@fieb.org.br'],
    'email_on_failure': True,
    'email_on_retries': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG(
        dag_id='windturbine_extraction',
        description='Extração de dados da turbina',
        schedule_interval=None,
        start_date=datetime(year=2024,month=6,day=1),
        catchup=False, tags=['windturbine','extraction'],
        default_args=default_args, default_view='graph',
        doc_md="## Dag para extrair dados em batch de turbina eólica")

for file_type in ['csv','json','parquet']:
    def process_file(file_type:str, **kwargs):
        file_path = Variable.get(f'{file_type}_path_file')
        
        match file_type:
            case 'json':
                with open(file_path) as file:
                    data = json.load(file)
                    kwargs['ti'].xcom_push(key=f'{file_type}_name', value=data['name'])
                    kwargs['ti'].xcom_push(key=f'{file_type}_idtemp', value=data['idtemp'])
                    kwargs['ti'].xcom_push(key=f'{file_type}_powerfactor', value=data['powerfactor'])
                    kwargs['ti'].xcom_push(key=f'{file_type}_hydraulicpressure', value=data['hydraulicpressure'])
                    kwargs['ti'].xcom_push(key=f'{file_type}_temperature', value=data['temperature'])
                    kwargs['ti'].xcom_push(key=f'{file_type}_timestamp', value=data['timestamp'])
                    
            case 'csv':
                data = pd.read_csv(file_path)
                if not data.empty:
                    row = data.iloc[0]
                    kwargs['ti'].xcom_push(key=f'{file_type}_name', value=row['name'])
                    kwargs['ti'].xcom_push(key=f'{file_type}_idtemp', value=row['idtemp'])
                    kwargs['ti'].xcom_push(key=f'{file_type}_powerfactor', value=row['powerfactor'])
                    kwargs['ti'].xcom_push(key=f'{file_type}_hydraulicpressure', value=row['hydraulicpressure'])
                    kwargs['ti'].xcom_push(key=f'{file_type}_temperature', value=row['temperature'])
                    kwargs['ti'].xcom_push(key=f'{file_type}_timestamp', value=row['timestamp'])
                
            case 'parquet':
                data = pd.read_parquet(file_path)
                if not data.empty:
                    row = data.iloc[0]
                    kwargs['ti'].xcom_push(key=f'{file_type}_name', value=row['name'])
                    kwargs['ti'].xcom_push(key=f'{file_type}_idtemp', value=row['idtemp'])
                    kwargs['ti'].xcom_push(key=f'{file_type}_powerfactor', value=row['powerfactor'])
                    kwargs['ti'].xcom_push(key=f'{file_type}_hydraulicpressure', value=row['hydraulicpressure'])
                    kwargs['ti'].xcom_push(key=f'{file_type}_temperature', value=row['temperature'])
                    kwargs['ti'].xcom_push(key=f'{file_type}_timestamp', value=row['timestamp'])
        
        os.remove(file_path)

    file_sensor_task = FileSensor(task_id=f'file_sensor_task_{file_type}',filepath=Variable.get(key=f'{file_type}_path_file'),fs_conn_id=f'fs_conn_{file_type}',poke_interval=10,dag=dag)
    get_data = PythonOperator(task_id=f'get_data_{file_type}',python_callable=process_file,provide_context=True,op_kwargs={'file_type':file_type},dag=dag)

    with TaskGroup(group_id=f'database_{file_type}',dag=dag) as database:
        sql = '''
        create table if not exists sensors (
            idtemp varchar,
            name varchar,
            powerfactor varchar,
            hydraulicpressure varchar,
            temperature varchar,
            timestamp varchar
        );
        '''
        create_table = PostgresOperator(task_id=f'create_table_{file_type}',postgres_conn_id='postgres',sql=sql, dag=dag)
        
        sql = '''
            INSERT INTO sensors (idtemp, name, powerfactor, hydraulicpressure, temperature, timestamp) VALUES (%s, %s, %s, %s, %s, %s)
        '''
        insert_data = PostgresOperator(task_id=f'insert_data_{file_type}',postgres_conn_id='postgres',parameters=(
            '{{ti.xcom_pull(task_ids="get_data_'+ file_type +'",key="'+ file_type + '_idtemp")}}',
            '{{ti.xcom_pull(task_ids="get_data_'+ file_type +'",key="'+ file_type + '_name")}}',
            '{{ti.xcom_pull(task_ids="get_data_'+ file_type +'",key="'+ file_type + '_powerfactor")}}',
            '{{ti.xcom_pull(task_ids="get_data_'+ file_type +'",key="'+ file_type + '_hydraulicpressure")}}',
            '{{ti.xcom_pull(task_ids="get_data_'+ file_type +'",key="'+ file_type + '_temperature")}}',
            '{{ti.xcom_pull(task_ids="get_data_'+ file_type +'",key="'+ file_type + '_timestamp") }}'
        ),sql=sql,dag=dag)

        create_table >> insert_data

    with TaskGroup(group_id=f'check_temp_{file_type}',dag=dag) as check_temp:
        send_email_alert = EmailOperator(task_id=f'send_email_alert_{file_type}',to='rafael.qsantos@fieb.org.br',subject='Airflow alert',
                                        html_content=f"<h3>Alerta de temperatura</h3><p>Dag: Windturbine {file_type}</p>",dag=dag)
        send_email_normal = EmailOperator(task_id=f'send_email_normal_{file_type}',to='rafael.qsantos@fieb.org.br',subject='Airflow advise',
                                        html_content=f"<h3>Temperatura Normal</h3><p>Dag: Windturbine {file_type}</p>",dag=dag)
        
        def avalia_temp(file_type:str,**kwarg):
            number = float(kwarg['ti'].xcom_pull(task_ids=f'get_data_{file_type}',key=f'{file_type}_temperature'))
            if number >= 24:
                return f'check_temp_{file_type}' + '.' + f'send_email_alert_{file_type}'
            else:
                return f'check_temp_{file_type}' + '.' + f'send_email_normal_{file_type}'

        check_temp_branch = BranchPythonOperator(task_id=f'check_temp_branch_{file_type}',python_callable=avalia_temp,op_kwargs={'file_type':file_type},provide_context=True,dag=dag)

        check_temp_branch >> [send_email_alert,send_email_normal]

    file_sensor_task >> get_data >> [database,check_temp] 
