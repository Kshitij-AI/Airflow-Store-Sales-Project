from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email_operator import EmailOperator

from data_cleaner import data_cleaner

yesterday_date = datetime.strftime(datetime.now()-timedelta(days=1), '%Y_%m_%d')

default_args = {
    'owner': 'Airflow',
    'start_date':datetime(2024,7,13),
    'retries': 1,
    'retry_delay': timedelta(seconds=15)
}

with DAG('store_sales_report',
         default_args=default_args,
         schedule_interval=timedelta(days=1),
         catchup=False,
         template_searchpath=['/usr/local/airflow/sql_files']) as dag:

    t1 = BashOperator(
        task_id='check_if_file_in_path',
        bash_command="shasum /usr/local/airflow/store_files_airflow/raw_store_transactions.csv",
        retries=2,
        retry_delay=timedelta(seconds=15))

    t2 = PythonOperator(
        task_id='clean_data',
        python_callable=data_cleaner
    )

    # SHOW VARIABLES LIKE "secure_file_priv"; --> mounted store_files in secure_file_priv path
    # ALTER USER 'root'@'%' IDENTIFIED WITH mysql_native_password BY 'root';
    t3 = MySqlOperator(
        task_id='create_table',
        mysql_conn_id='mysql_conn',
        sql='create_table.sql'
    )

    t4 = MySqlOperator(
        task_id='insert_data_into_table',
        mysql_conn_id='mysql_conn',
        sql='insert_into_table.sql'
    )

    t5 = MySqlOperator(
        task_id='select_from_table',
        mysql_conn_id='mysql_conn',
        sql='select_from_table.sql'
    )

    t6 = BashOperator(
        task_id='rename_store_wise_profit_file',
        bash_command='mv /usr/local/airflow/store_files_airflow/store_wise_profit.csv /usr/local/airflow/store_files_airflow/store_wise_profit_%s.csv' %yesterday_date
    )

    t7 = BashOperator(
        task_id='rename_location_wise_profit_file',
        bash_command='mv /usr/local/airflow/store_files_airflow/location_wise_profit.csv /usr/local/airflow/store_files_airflow/location_wise_profit_%s.csv' %yesterday_date
    )

    t8 = MySqlOperator(
        task_id='drop_db',
        mysql_conn_id='mysql_conn',
        sql='drop_db.sql'
    )

    t9 = EmailOperator(
        task_id='email_report',
        to='kshitijpop@gmail.com',
        subject='[Notification] - Daily Profit Report',
        html_content='Hi Kshitij,<BR><BR>Daily profit reports are ready. Please find the attachments.<BR><BR>Thanks & Regards,<BR>Airflow Team',
        # files=['/usr/local/airflow/store_files_airflow/store_wise_profit_%s.csv' % yesterday_date,
        #        '/usr/local/airflow/store_files_airflow/location_wise_profit_%s.csv' % yesterday_date]
    )

    # As next day data will come again with same name
    t10 = BashOperator(
        task_id='rename_raw_file',
        bash_command='mv ~/store_files_airflow/raw_store_transactions.csv ~/store_files_airflow/raw_store_transactions_%s.csv' %yesterday_date
    )

    t1>>t2>>t3>>t4>>t5>>[t6,t7,t8]>>t9>>t10