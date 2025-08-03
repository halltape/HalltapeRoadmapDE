from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from hashlib import md5
from sqlalchemy import create_engine
import pandas as pd


default_args = {
  'owner': '@Shust_DE',
  'depends_on_past': False,
  'start_date': datetime(2024, 11, 13),
  'email': ['https://t.me/Shust_DE'],
  'schedule_interval': "@hourly",
}


def _generate_file(**kwargs):
  ti = kwargs['ti']
  path_filename = '/tmp/data.csv'
  table = [(i, md5(int(i).to_bytes(8, 'big', signed=True)).hexdigest()) for i in range(1, 100)]
  table = pd.DataFrame(table, columns=['id', 'md5_id'])
  table.to_csv(path_filename, index=False)
  ti.xcom_push(key='path_file', value=path_filename)


def _data_in_postgres(**kwargs):
  ti = kwargs['ti']
  df = pd.read_csv("/tmp/processed_data/data.csv")
  engine = create_engine('postgresql://user:user@host.docker.internal:5431/user')
  df.to_sql('table_name', engine, if_exists='append', schema='public', index=False)
  ti.xcom_push(key='count_string', value=len(df))


dag = DAG(
    dag_id="load_file_to_psql",
    default_args=default_args
)

generate_file = PythonOperator(
    task_id='generate_file',
    python_callable=_generate_file,
  dag=dag,
)


move_data_file = BashOperator(
  task_id="move_data_file",
  bash_command=("mkdir -p /tmp/processed_data/ && "
          "mv {{ ti.xcom_pull(task_ids='generate_file', key='path_file') }} /tmp/processed_data/"),
  dag=dag,
)


create_table_psql = PostgresOperator(
	task_id='create_table',
        postgres_conn_id='psql_connection',
        sql=""" DROP TABLE IF EXISTS table_name;
                CREATE TABLE table_name ( id int, 
                             md5_id text); """
)


data_in_postgres = PythonOperator(
    task_id='data_in_postgres',
    python_callable=_data_in_postgres,
  dag=dag,
)


print_count_string_in_df = BashOperator(
  task_id="print_count_string_in_df",
  bash_command=('''echo "В таблице {{ ti.xcom_pull(task_ids='data_in_postgres', key='count_string') }} строк" '''),
  dag=dag,
)


(
    generate_file >> 
    move_data_file >> 
    create_table_psql >> 
    data_in_postgres >> 
    print_count_string_in_df
)
