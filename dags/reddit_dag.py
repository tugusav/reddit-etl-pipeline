from airflow import DAG
from datetime import datetime
import os
import sys

# Add the parent directory to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow.operators.python_operator import PythonOperator
from pipelines.reddit_pipeline import reddit_pipeline
from pipelines.aws_s3_pipeline import upload_s3_pipeline

default_args = {
    'owner': 'tugus',
    'start_date': datetime(2024, 2, 15),
}

file_postfix = datetime.now().strftime("%Y%m%d")

dag = DAG(
    dag_id = 'etl_reddit_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['reddit', 'etl', 'pipeline']
)


extract = PythonOperator(
    task_id='extract_reddit',
    python_callable=reddit_pipeline,
    dag=dag,
    op_kwargs={
        'file_name': f'reddit_{file_postfix}.csv',
        'subreddit': 'football',
        'time_filter': 'month',
        'limit': 100
    }
)

# upload to S3 bucket

upload_s3 = PythonOperator(
    task_id='s3_upload',
    python_callable=upload_s3_pipeline,
    provide_context=True,
    dag=dag

)
extract >> upload_s3