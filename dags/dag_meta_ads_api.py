import pandas as pd
import requests
import json
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

from api.meta_ads import MetaAPI
from handles.mysql_handle import MySQLHandle

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago

load_dotenv()

meta_token = os.getenv('meta_token')
ad_acc = os.getenv('ad_acc')

host = os.getenv('HOST')
port = os.getenv('PORT')
user = os.getenv('USER')
passwd = os.getenv('PASSWORD')
database = os.getenv('DATABASE')

meta_api = MetaAPI(meta_token, ad_acc)
mysql_handle = MySQLHandle(host,
                                port,
                                user,
                                passwd,
                                database)

status_fields = ['id','status','objective','created_time']
content_fields = ['adset_id','adset_name','campaign_id','campaign_name','spend','objective']
actions_fields= ['adset_id','actions']
destination_table = 'campaigns'

def request_adset():
    status_json = meta_api.adsets_status(status_fields)
    status_json = json.loads(status_json._content.decode('utf-8'))
    print(status_json)
    status = pd.json_normalize(status_json['data'])

    status = status.rename(columns={'id': 'adset_id'})
    status['created_time'] = pd.to_datetime(status['created_time'])
    status['created_time'] = status['created_time'].dt.strftime("%Y-%m-%d %H:%M:%S")

    return status

def request_content():
    content_json = meta_api.adsets_content(content_fields)
    content_json = json.loads(content_json._content.decode('utf-8'))
    content_df = pd.json_normalize(content_json['data'])

    status = request_adset()
    content = pd.merge(content_df, status, on='adset_id', how='inner')

    return content

def request_actions():
    actions_json = meta_api.adsets_actions(actions_fields)
    actions_json = json.loads(actions_json._content.decode('utf-8'))
    actions_df = pd.json_normalize(actions_json['data'], 'actions', ['adset_id'])

    actions_df['value'] = pd.to_numeric(actions_df['value'])
    actions_df = actions_df.query('action_type == "lead"')

    actions = actions_df.pivot_table(values="value", 
                                    index="adset_id", 
                                    columns=['action_type'])

    actions = actions.rename(columns={'lead': 'leads'})

    content = request_content()
    final_data = content.join(actions, on='adset_id')

    return final_data

def truncate_table():
    mysql_handle.create_connection()
    mysql_handle.query_data(query=f'TRUNCATE TABLE {destination_table}')

def insert_table():
    engine = create_engine(f"mysql+mysqldb://{user}:{passwd}@{host}:{port}/{database}")
    con_engine = engine
    data = request_actions()
    print(data)
    data.to_sql(f'{destination_table}', con=con_engine, if_exists='append', index=False)

# DAG
with DAG(
    dag_id='dag_meta_ads_api', 
    description="Meta ADS API",
    schedule_interval= "0 * * * *",
    start_date = days_ago(1),
    fail_stop=True,
    catchup=False
) as dag:

    begin = DummyOperator(
        task_id="begin",
        trigger_rule='all_success'
    )

    task_request_adset = PythonOperator(
        task_id="task_request_adset",
        python_callable=request_adset,
        provide_context=True,
        trigger_rule='all_success'
    )

    task_request_content = PythonOperator(
        task_id="task_request_content",
        python_callable=request_content,
        provide_context=True,
        trigger_rule='all_success'
    )

    task_request_actions = PythonOperator(
        task_id="task_request_actions",
        python_callable=request_actions,
        provide_context=True,
        trigger_rule='all_success'
    )

    task_truncate_table = PythonOperator(
        task_id="task_truncate_table",
        python_callable=truncate_table,
        provide_context=True,
        trigger_rule='all_success'
    )

    task_insert_table = PythonOperator(
        task_id="task_insert_table",
        python_callable=insert_table,
        provide_context=True,
        trigger_rule='all_success'
    )

    email_on_failed = EmailOperator(
        task_id="email_failed_task",
        to='email_teste',
        subject=f"Falha na atualização da tabela de {destination_table}!",
        html_content=f"<i>A Atualização da tabela de {destination_table} falhou, confira o log no webserver para entender melhor o problema</i>",
        cc=['email_cc'],
        trigger_rule='one_failed'
    )
    
    end = DummyOperator(
        task_id="end",
        trigger_rule='all_success'
    )

begin >> task_request_adset >> task_request_content >> task_request_actions >> task_truncate_table >> task_insert_table >> email_on_failed >> end
