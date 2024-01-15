import json
import boto3
import time
import subprocess
from send_email import send_email


def lambda_handler(event, context):
    s3_file_list = []
    
    s3_client=boto3.client('s3')
    for object in s3_client.list_objects_v2(Bucket='midterm-data-dump-umer')['Contents']:
        s3_file_list.append(object['Key'])
    
    s3_file_list = sorted(s3_file_list)

    datestr = time.strftime("%Y%m%d")

    required_file_list = [f'calendar_{datestr}.csv', f'inventory_{datestr}.csv', f'product_{datestr}.csv', f'sales_{datestr}.csv', f'store_{datestr}.csv']

    # scan S3 bucket
    
    if all(object in s3_file_list for object in required_file_list):
        s3_file_url = ['s3://' + 'midterm-data-dump-umer/' + a for a in required_file_list]
        table_name = ([a[:-13] for a in required_file_list])
        data = json.dumps({"conf":{category: url for url, category in zip(s3_file_url, table_name)}})
        
    # send signal to Airflow    
        endpoint= 'http://15.156.167.182:8080/api/v1/dags/midterm_dag/dagRuns'
    
        subprocess.run([
            'curl',
            '-X', 
            'POST',
            endpoint,
            '-H',
            'Content-Type: application/json',
            '--user',
            'airflow:airflow',
            '--data', 
            data])
    
        print('File are send to Airflow')
    else:
        send_email()
