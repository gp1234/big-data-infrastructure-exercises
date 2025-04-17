from datetime import datetime, timedelta
import os
import aiohttp
import asyncio
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.transfers.http_to_s3 import HttpToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'aircraft_data_download',
    default_args=default_args,
    description='Download aircraft data and store in S3',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['aircraft', 'download'],
)

# Get configuration from Airflow variables
S3_BUCKET = Variable.get('s3_bucket', default_var='your-bucket-name')
SOURCE_URL = Variable.get('source_url', default_var='https://samples.adsbexchange.com/readsb-hist')
FILE_LIMIT = Variable.get('file_limit', default_var='100')

def get_file_links(**context):
    """Get list of file links to download"""
    base_url = f"{SOURCE_URL}/2023/11/01/"
    
    async def fetch_links():
        async with aiohttp.ClientSession() as session:
            async with session.get(base_url) as response:
                html = await response.text()
                soup = BeautifulSoup(html, "lxml")
                links = soup.find_all("a", href=True)
                file_links = [link["href"] for link in links if link["href"].endswith(".json.gz")]
                return file_links[:int(FILE_LIMIT)]

    # Run the async function
    loop = asyncio.get_event_loop()
    file_links = loop.run_until_complete(fetch_links())
    
    # Store the links in XCom for downstream tasks
    context['task_instance'].xcom_push(key='file_links', value=file_links)
    return file_links

# Task to delete existing files in S3
delete_s3_files = S3DeleteObjectsOperator(
    task_id='delete_s3_files',
    bucket=S3_BUCKET,
    prefix='data/raw/day=20231101/',
    dag=dag,
)

# Task to get file links
get_links = PythonOperator(
    task_id='get_file_links',
    python_callable=get_file_links,
    provide_context=True,
    dag=dag,
)

# Dynamic task to download and upload files
def create_download_tasks(**context):
    file_links = context['task_instance'].xcom_pull(task_ids='get_file_links', key='file_links')
    tasks = []
    
    for file_name in file_links:
        file_url = f"{SOURCE_URL}/2023/11/01/{file_name}"
        s3_key = f"data/raw/day=20231101/{file_name}"
        
        download_task = HttpToS3Operator(
            task_id=f'download_{file_name.replace(".", "_")}',
            http_conn_id='http_default',
            endpoint=file_url,
            s3_conn_id='aws_default',
            s3_bucket=S3_BUCKET,
            s3_key=s3_key,
            dag=dag,
        )
        tasks.append(download_task)
    
    return tasks

# Set task dependencies
delete_s3_files >> get_links >> create_download_tasks() 