import pendulum
from airflow import DAG
from airflow.decorators import dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from jinja2 import Environment
from datetime import datetime as dt
from airflow.models import Variable


PATH_TO_PROJECT = '/opt/airflow/dags/'

def normaling_datetime(ts_nodash):
    print(ts_nodash, type(ts_nodash))
    date = dt.strptime(ts_nodash, '%Y%m%dT%H%M%S')
    return str(date.strftime('%Y-%m-%d %H:%M:%S'))

def get_column_from_table(path_sql):
        postgres_hook = PostgresHook(postgres_conn_id="postgres-test")
        conn = postgres_hook.get_conn()
        sql = ''
        with open(path_sql) as ps:
              for line in ps:
                    sql += line
        cursor = conn.cursor()
        cursor.execute(sql)
        res = []
        for line in cursor:
            res+= line
        return ','.join(res)

def insert_data_with_J2_params(path_sql, table_name, columns, values):
        phook = PostgresHook(postgres_conn_id="postgres-test")
        conn = phook.get_conn()
        presql = ''
        with open(path_sql) as ps:
              for line in ps:
                    presql+= line
        # print(values)
        env = Environment()
        template = env.from_string(presql)
        sql = template.render(table_name = table_name, columns = columns, values = values)
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        conn.close()

def telegram_alert(self):
     from telebot import Telegram_log as tl


     token = Variable.get("tgm_tok")
     chat_id = Variable.get("tgm_chat_id")
     tl(token, chat_id).logger('elcar_stat_alert')

def set_data_in_database(**kwargs ):
      with open(kwargs['path_to_dataset'], 'r') as ds:
            values = ''
            date_today = normaling_datetime(kwargs['date_today'])
            columns = get_column_from_table(f'{PATH_TO_PROJECT}/sql/get_column_t.sql')
            next(ds)
            start_date = dt.strptime(kwargs['start_date'],'%d.%m.%Y')
            last_date = dt.strptime(kwargs['last_date'],'%d.%m.%Y')
            rows = 0
            for line in ds:
                  if rows < 1000:
                        prevalue = line.replace('\'','@@').replace(', ', '~~~').rstrip('\n').split(',')
                        # .replace(',,', ',~~~,').replace(',\n',',~~~\n')
                        date = dt.strptime(prevalue[1],'%Y.%m')
                        if len(prevalue)>14:
                              print(prevalue)
                        if start_date < date < last_date:
                              if prevalue[2] == '':
                                    prevalue[2] = '@@'
                              prevalue[1] = str(date.strftime('%Y-%m-%d'))
                              prevalue.append(date_today)
                              values += str(tuple(prevalue)) + ','
                              rows += 1
                  if rows == 1000:
                        insert_data_with_J2_params(f"{PATH_TO_PROJECT}sql/insert_into_table.sql",'cards_stats', columns, values.replace('@@','\'\'').replace('\'~~~\'', ', ').rstrip(','))
                        rows = 0
                        values = ''
            if rows > 0 :
                 insert_data_with_J2_params(f'{PATH_TO_PROJECT}sql/insert_into_table.sql','cards_stats', columns, values.replace('@@','\'\'').replace('\'~~~\'', ', ').rstrip(','))

dag = DAG(
    dag_id = "electronic_cards_transactions",
    start_date = pendulum.datetime(2024, 2, 5),
    schedule_interval = "*/30 * * * * ",
    catchup=True,
    tags = ["test"]
)
create_ect_stats_table = SQLExecuteQueryOperator(
      task_id = 'create_electronic_cards_stats_table_idexists', 
      conn_id = "postgres-test", 
      sql = 'sql/create_table.sql', 
      dag=dag)

insert_into_table_by_date = PythonOperator(
      task_id = 'insert',
      python_callable = set_data_in_database,
      on_failure_callback = telegram_alert,
      provide_context = True,
      op_kwargs = { "path_to_dataset" : "/opt/airflow/dags/dataset/electronic-card-transactions-december-2023-csv-tables.csv",
          "date_today" : "{{ts_nodash}}",
          "start_date" : Variable.get("start_date_period"),
          "last_date" : Variable.get("end_date_period")},
     dag=dag
)

create_ect_stats_table >> insert_into_table_by_date