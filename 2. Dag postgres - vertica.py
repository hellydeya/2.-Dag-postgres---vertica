from airflow import DAG
import logging
import vertica_python
import json
import pandas as pd
from airflow.operators.python_operator import PythonOperator
from airflow.models.variable import Variable
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
from airflow.sensors.filesystem import FileSensor
from airflow.operators.dummy_operator import DummyOperator
from time import sleep
from airflow import AirflowException

logging = logging.getLogger(name)

connection_pg = PostgresHook('PG_SOURCE_CONNECTION').get_conn()
connection_vertica = vertica_python.connect(**json.loads(Variable.get("VERTICA_CONNECTION")))

sql_path = Variable.get("sql_path")
temp_file_path = Variable.get("temp_file_path")

def sql_read_file(file_name):
try:
fn = f'{sql_path}{file_name}.sql'
with open(fn, 'r') as sql:
return sql.read()
except Exception as e:
logging.warning(f"CAN'T READ SQL FILE: {file_name} {fn} .....EXCEPTION: {e}")

def read_table(table_name):
sql = 'read_currencies' if 'curr' in table_name else 'read_transactions'
try:
df = pd.read_sql(sql_read_file(sql), connection_pg)
return df
except Exception as e:
logging.warning(f"TABLE DON'T READ, EXCEPTION: {e}")

def write_csv(file_name):
file_name = 'currencies' if 'curr' in file_name else 'transactions'
df = read_table(file_name)

if isinstance(df, pd.DataFrame):
    try:
        df.to_csv(f"{temp_file_path}{file_name}.csv", sep=',', encoding='utf-8', header=False, index=False, mode='w')
    except Exception as e:
        logging.warning(f"CSV FILE DON'T WRITE, EXCEPTION: {e}")
def load_to_stagging(table_name):
query = (sql_read_file('stg_load_currencies_local').format(f"{temp_file_path}currencies.csv") if 'curr' in table_name else sql_read_file('stg_load_transactions_local').format(f"{temp_file_path}transactions.csv"))
if isinstance(query, str):
try:
with connection_vertica.cursor() as cursor:
cursor.execute(sql_read_file('stg_truncate_currencies')) if 'curr' in table_name else cursor.execute(sql_read_file('stg_truncate_transactions'))
cursor.execute(query)
except Exception as e:
logging.warning(f"SOMETHING WRONG! EXCEPTION: {e}")

def dwh_load(ds):
try:
with connection_vertica.cursor() as cursor:
res = cursor.execute(sql_read_file("dwh_load").format(ds,ds))
logging.info(f"DATA IS LOADED ON: {ds} - {cursor.fetchone()[0]} ROW") if res else logging.warning(f"NO DATA ON: {ds}")
except Exception as e:
logging.warning(f"SOMETHING WRONG! EXCEPTION: {e}")

def wait_data(table_name, ds):
query = sql_read_file('wait_data_currencies') if 'curr' in table_name else sql_read_file('wait_data_transactions')

time_sleep = 0
data_in_cursor = None
while not data_in_cursor:
    try:
        with connection_vertica.cursor() as cursor:      
            cursor.execute(query.format(ds))
            if cursor.fetchone(): data_in_cursor = cursor.fetchone()[0]
    except Exception as e:
        logging.warning(f"SOMETHING WRONG! EXCEPTION: {e}") 
    sleep(1)
    time_sleep += 1
    if time_sleep > 60: 
        raise ValueError('TIMEOUT....MAYBE NO DATA?!?')
def load_csv_currencies():
write_csv('curr')

def load_csv_transactions():
write_csv('trans')

def load_transactions_to_staging():
load_to_stagging('trans')

def load_currencies_to_staging():
load_to_stagging('curr')

def wait_data_transaction(ds):
wait_data('trans', ds)

def wait_data_currencies(ds):
wait_data('curr', ds)

with DAG(
'DAG_pg_to_dwh',
tags=['DAG_pg_to_dwh'],
start_date=datetime(2022, 10, 1),
end_date=datetime(2022, 10, 31),
schedule_interval="@daily",
max_active_runs=1,
catchup = True
) as dag:

start = DummyOperator(
    task_id="start")

load_csv_currencies = PythonOperator(
    task_id='load_csv_currencies',
    provide_context=True,
    python_callable=load_csv_currencies)

load_csv_transactions = PythonOperator(
    task_id='load_csv_transactions',
    provide_context=True,
    python_callable=load_csv_transactions)

wait_file_currencies = FileSensor(
    task_id="wait_file_currencies",
    filepath=f'{temp_file_path}currencies.csv',
    poke_interval= 20)

wait_file_transaction = FileSensor(
    task_id="wait_file_transaction",
    filepath=f'{temp_file_path}transactions.csv',
    poke_interval= 20)

load_currencies_to_staging = PythonOperator(
    task_id='load_currencies_to_staging',
    provide_context=True,
    python_callable=load_currencies_to_staging)

load_transactions_to_staging = PythonOperator(
    task_id='load_transactions_to_staging',
    provide_context=True,
    python_callable=load_transactions_to_staging)

wait_data_transaction = PythonOperator(
    task_id='wait_data_transaction',
    provide_context=True,
    python_callable=wait_data_transaction)

wait_data_currencies = PythonOperator(
    task_id='wait_data_currencies',
    provide_context=True,
    python_callable=wait_data_currencies)

dwh_load = PythonOperator(
    task_id='dwh_load',
    provide_context=True,
    python_callable=dwh_load)
load_csv_currencies.set_upstream(start)
load_csv_transactions.set_upstream(start)
wait_file_currencies.set_upstream(load_csv_currencies)
wait_file_transaction.set_upstream(load_csv_transactions)
load_currencies_to_staging.set_upstream(wait_file_currencies)
load_transactions_to_staging.set_upstream(wait_file_transaction)
wait_data_currencies.set_upstream(load_currencies_to_staging)
wait_data_transaction.set_upstream(load_transactions_to_staging)
dwh_load.set_upstream(wait_data_currencies)
dwh_load.set_upstream(wait_data_transaction)
