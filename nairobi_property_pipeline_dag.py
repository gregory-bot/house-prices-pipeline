from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'kipngenogregory',
    'depends_on_past': False,
    'email': ['kipngenogregory@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'nairobi_property_pipeline',
    default_args=default_args,
    description='Weekly Nairobi property data pipeline',
    schedule_interval='0 8 * * MON',
    start_date=datetime(2026, 2, 23),
    catchup=False,
)

scrape = BashOperator(
    task_id='scrape_listings',
    bash_command='C:/Users/kipng/nairobi_property_pricing/.venv/Scripts/python.exe C:/Users/kipng/nairobi_property_pricing/scrape_listings.py',
    dag=dag,
)

clean = BashOperator(
    task_id='clean_properties',
    bash_command='C:/Users/kipng/nairobi_property_pricing/.venv/Scripts/python.exe C:/Users/kipng/nairobi_property_pricing/clean_properties.py',
    dag=dag,
)

prepare = BashOperator(
    task_id='prepare_properties',
    bash_command='C:/Users/kipng/nairobi_property_pricing/.venv/Scripts/python.exe C:/Users/kipng/nairobi_property_pricing/prepare_properties.py',
    dag=dag,
)

notify = EmailOperator(
    task_id='send_email',
    to='kipngenogregory@gmail.com',
    subject='Nairobi Property Data Pipeline Update',
    html_content='Weekly property data scraping, cleaning, and upload to PostgreSQL completed successfully.',
    dag=dag,
)

scrape >> clean >> prepare >> notify
