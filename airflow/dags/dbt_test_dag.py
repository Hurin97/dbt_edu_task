import pendulum
from airflow import DAG
from airflow.decorators import dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from jinja2 import Environment
from datetime import datetime as dt
from airflow.models import Variable
from airflow_dbt.operators.dbt_operator import DbtRunOperator


PATH_TO_PROJECT = '/opt/airflow/dags/'

def telegram_alert(self):
     from telebot import Telegram_log as tl


     token = Variable.get("tgm_tok")
     chat_id = Variable.get("tgm_chat_id")
     tl(token, chat_id).logger('elcar_stat_alert')


dag = DAG(
    dag_id = "electronic_cards_transactions_dbt",
    start_date = pendulum.datetime(2024, 2, 12),
    schedule_interval = "*/30 * * * * ",
    catchup=True,
    tags = ["test"]
)

dbt_something = DbtRunOperator(
     task_id ='dbt_smthg',
     dir = '/usr/app/project',
     profiles_dir = '/usr/app/dbt-profiles',
     vars = {'first_date': Variable.get("start_date_period"),
             'last_date': Variable.get("end_date_period")},
     dag = dag
     
)

dbt_something 

# vars = {'first_date': dt.strptime(Variable.get("start_date_period"),'%d.%m.%Y'),
#              'last_date': dt.strptime(Variable.get("end_date_period"),'%d.%m.%Y')},
     