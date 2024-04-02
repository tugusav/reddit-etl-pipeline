from etls.aws_s3_etl import connect_to_s3, create_bucket_if_not_exists, upload_file
from utils.constants import AWS_BUCKET_NAME, AWS_REGION

def upload_s3_pipeline(ti):
    file_path = ti.xcom_pull(task_ids='extract_reddit', key='return_value')
    
    # If file_path is a bytes object, decode it to a string
    if isinstance(file_path, bytes):
        file_path = file_path.decode('utf-8')

    s3 = connect_to_s3()
    create_bucket_if_not_exists(s3, AWS_BUCKET_NAME, AWS_REGION)
    upload_file(s3, file_path, AWS_BUCKET_NAME, file_path.split('/')[-1])