
import json
import logging
import os
import time
import boto3

logging.getLogger().setLevel(logging.INFO)

def get_s3_client():
    return boto3.client('s3')

def get_athena_client():
    return boto3.client('athena')

def get_step_function_client():
    return boto3.client('stepfunctions')

def get_sns_client():
    return boto3.client('sns')

def get_env_variable(var_name: str) -> str:
    return os.environ[var_name]

def fetch_query_from_s3(bucket_name: str, query_key: str) -> str:
    response = get_s3_client().get_object(Bucket=bucket_name, Key=query_key)
    return response['Body'].read().decode('utf-8')

def execute_athena_query(query: str, query_bucket_name: str, output_folder: str) -> dict:
    return get_athena_client().start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': 'default'},
        ResultConfiguration={'OutputLocation': f's3://{query_bucket_name}/{output_folder}'}
    )

def check_athena_query_status(query_execution_id: str) -> str:
    response = get_athena_client().get_query_execution(QueryExecutionId=query_execution_id)
    return response['QueryExecution']['Status']['State']

def copy_file_in_s3(source_bucket: str, source_key: str, target_bucket: str, target_key: str) -> None:
    get_s3_client().copy_object(
        Bucket=target_bucket,
        CopySource={'Bucket': source_bucket, 'Key': source_key},
        Key=target_key
    )

def start_step_function_execution(input_payload: dict) -> dict:
    return get_step_function_client().start_execution(
        stateMachineArn=get_env_variable('stateMachineArn'),
        input=json.dumps(input_payload)
    )

def send_alert_on_failure(topic_arn: str, message: str) -> None:
    get_sns_client().publish(
        TopicArn=topic_arn,
        Message=message
    )

def lambda_handler(event: dict, context: dict) -> None:
    try:
        logging.info(f"Event: {event}")
        
        year = int(event['year'])
        month = int(event['month'])
        day = int(event['day'])
        
        query_key = get_env_variable('query_key')
        bucket_name = get_env_variable('bucket_name')
        query_bucket_name = get_env_variable('query_bucket_name')
        keys = []
        
        for horizon in range(1, 4):
            query = fetch_query_from_s3(bucket_name, query_key)
            response = execute_athena_query(query, query_bucket_name, 'reference_data')
            query_id = response['QueryExecutionId']
            logging.info(f"Query id for horizon {horizon}: {query_id}")
            
            while True:
                status = check_athena_query_status(query_id)
                if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break
                logging.info("Sleeping for 5 seconds")
                time.sleep(5)

            key = f"processing/incoming/year={year}/month={month}/day={day}/PurchaseAssetsBySegment_h{horizon}_{month:02d}{day:02d}{str(year)[-2]}.csv"
            copy_file_in_s3(query_bucket_name, f"query_results/{query_id}.csv", bucket_name, key)
            keys.append(key)
            
        payload = {'bucket_name': bucket_name, 'key': keys, 'file_date': f'{month:02d}{day:02d}{str(year)[-2]}'}
        logging.info(f"Payload: {payload}")
        start_step_function_execution(payload)
    except Exception as raised_exception:
        logging.critical(f"Exception: {raised_exception}")
        send_alert_on_failure(get_env_variable('snsTopicArn'), f"Exception: {raised_exception}\nEvent: {event}")
        raise raised_exception
