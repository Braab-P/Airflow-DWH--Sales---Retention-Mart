import time
import requests
import json
import pandas as pd

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook


http_conn = HttpHook.get_connection('http_conn_id')
api_key = http_conn.extra_dejson.get('api_key')
base_url = http_conn.host

postgres_conn_id = 'postgresql_de'

nickname = os.getenv('NICKNAME', 'your_nickname')
cohort = os.getenv('COHORT', 'your_cohort')

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}


def generate_report(ti):
    response = requests.post(f'{base_url}/generate_report', headers=headers)
    response.raise_for_status()
    task_id = response.json()['task_id']
    ti.xcom_push(key='task_id', value=task_id)


def get_report(ti):
    task_id = ti.xcom_pull(key='task_id')

    report_id = None
    for _ in range(20):
        response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=headers)
        response.raise_for_status()
        data = response.json()

        if data['status'] == 'SUCCESS':
            report_id = data['data']['report_id']
            break

        time.sleep(10)

    if not report_id:
        raise TimeoutError("Report was not generated")

    ti.xcom_push(key='report_id', value=report_id)


def get_increment(date, ti):
    report_id = ti.xcom_pull(key='report_id')

    response = requests.get(
        f'{base_url}/get_increment?report_id={report_id}&date={str(date)}T00:00:00',
        headers=headers
    )
    response.raise_for_status()

    data = response.json()
    increment_id = data['data']['increment_id']

    if not increment_id:
        raise ValueError("Increment not found")

    ti.xcom_push(key='increment_id', value=increment_id)


def upload_data_to_staging(filename, date, pg_table, pg_schema, ti):
    increment_id = ti.xcom_pull(key='increment_id')

    s3_url = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{increment_id}/{filename}'
    local_file = f"{date.replace('-', '')}_{filename}"

    response = requests.get(s3_url)
    response.raise_for_status()

    with open(local_file, "wb") as f:
        f.write(response.content)

    df = pd.read_csv(local_file)

    # чистим
    if 'id' in df.columns:
        df = df.drop('id', axis=1)

    df = df.drop_duplicates(subset=['uniq_id'])

    # backward compatibility
    if 'status' not in df.columns:
        df['status'] = 'shipped'

    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()

    df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)


default_args = {
    "owner": "student",
    "retries": 0
}

business_dt = '{{ ds }}'

with DAG(
    dag_id='sales_mart',
    default_args=default_args,
    start_date=datetime.today() - timedelta(days=7),
    end_date=datetime.today() - timedelta(days=1),
    schedule_interval='@daily',
    catchup=True
) as dag:

    # API
    t_generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report
    )

    t_get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report
    )

    t_get_increment = PythonOperator(
        task_id='get_increment',
        python_callable=get_increment,
        op_kwargs={'date': business_dt}
    )

    t_upload_user_order = PythonOperator(
        task_id='upload_user_order_inc',
        python_callable=upload_data_to_staging,
        op_kwargs={
            'date': business_dt,
            'filename': 'user_order_log_inc.csv',
            'pg_table': 'user_order_log',
            'pg_schema': 'staging'
        }
    )


    t_update_d_item = PostgresOperator(
        task_id='update_d_item',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_item.sql"
    )

    t_update_d_customer = PostgresOperator(
        task_id='update_d_customer',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_customer.sql"
    )

    t_update_d_city = PostgresOperator(
        task_id='update_d_city',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_city.sql"
    )

    t_update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_sales.sql"
    )

    t_update_retention = PostgresOperator(
        task_id='update_f_customer_retention',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_customer_retention.sql"
    )

    (
        t_generate_report
        >> t_get_report
        >> t_get_increment
        >> t_upload_user_order
        >> [t_update_d_item, t_update_d_customer, t_update_d_city]
        >> t_update_f_sales
        >> t_update_retention
    )