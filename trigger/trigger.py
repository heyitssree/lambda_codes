"""Lambda to trigger step function."""
import json
import logging
import os
import time
import boto3

logging.getLogger().setLevel(logging.INFO)


def lambda_handler(event: dict, context: dict) -> None:
    try:
        logging.info(f"Event: {event}")

        year = int(event['year'])
        month = int(event['month'])
        day = int(event['day'])

        query_key = os.environ['query_key']
        bucket_name = os.environ['bucket_name']
        query_bucket_name = os.environ['query_bucket_name']
        keys = []

        for horizon in range(1, 4):
            query = query_generator(
                year, month, day, horizon, query_key, bucket_name)
            query_id = query_execution(
                query, query_bucket_name, 'reference_data')
            logging.info(f"Query id for horizon {horizon}: {query_id}")
            key = f"processing/incoming/year={year}/month={month}/day={day}/PurchaseAssetsBySegment_h{horizon}_{month:02d}{day:02d}{str(year)[-2]}.csv"
            s3_client = boto3.client('s3')
            s3_client.copy_object(
                Bucket=bucket_name,
                CopySource={'Bucket': query_bucket_name,
                            'Key': f"query_results/{query_id}.csv"},
                Key=key)
            keys.append(key)

        payload = {'bucket_name': bucket_name, 'key': keys,
                   'file_date': f'{month:02d}{day:02d}{str(year)[-2]}'}
        logging.info(f"Payload: {payload}")
        execute_step_function(payload)
    except Exception as raised_exception:
        logging.critical(f"Exception: {raised_exception}")
        publish_alert_on_failure(event, raised_exception)
        raise raised_exception


def query_generator(year: int, month: int, day: int, horizon: int, query_key: str, bucket_name: str) -> str:
    try:
        """Generate query for given date and horizon."""
        s3_client = boto3.client('s3')
        query = s3_client.get_object(Bucket=bucket_name, Key=query_key)[
            'Body'].read().decode('utf-8')
        query = query.replace('@year', str(year))
        query = query.replace('@month', str(month))
        query = query.replace('@day', str(day))
        query = query.replace('@horizon', str(horizon))

        return query
    except Exception as raised_exception:
        logging.critical(f"Exception: {raised_exception}")
        raise raised_exception


def query_execution(query: str, query_bucket: str, database: str) -> None:
    try:
        athena_client = boto3.client('athena')
        query_execution = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database,
                'Catalog': 'AwsDataCatalog'},
            ResultConfiguration={
                'OutputLocation': f's3://{query_bucket}',
                'EncryptionConfiguration': {
                    'EncryptionOption': 'SSE_S3'
                }},
            WorkGroup='Ga-alfa-forecast-output-workgroup')
        query_execution_id = query_execution['QueryExecutionId']

        while (True):
            logging.info("Checking status of Athena query execution")
            response = athena_client.get_query_execution(
                QueryExecutionId=query_execution_id)
            if response['QueryExecution']['Status']['State'] == 'SUCCEEDED':
                logging.info("Query execution succeeded")
                break
            elif response['QueryExecution']['Status']['State'] == 'FAILED':
                logging.info("Query execution failed")
                raise Exception("Query execution failed")
                break
            elif response['QueryExecution']['Status']['State'] == 'CANCELLED':
                logging.info("Query execution cancelled")
                raise Exception("Query execution cancelled")
                break
            logging.info("Sleeping for 5 seconds")
            time.sleep(5)
        response = athena_client.get_query_results(
            QueryExecutionId=query_execution_id)
        logging.info(f"Query results: {response}")
        return query_execution_id
    except Exception as raised_exception:
        logging.critical(f"Exception: {raised_exception}")
        raise raised_exception


def execute_step_function(file_details: dict) -> None:
    try:
        step_function_client = boto3.client('stepfunctions')
        logging.info("Starting step function execution")
        response = step_function_client.start_execution(
            stateMachineArn=os.getenv('stateMachineArn'),
            input=json.dumps(file_details))
        if not response.get('executionArn'):
            logger.critical("Failed to initiate step function")
            raise Exception("Failed to initiate step function")
        logging.info(f"Step function execution response: {response}")
        return response
    except Exception as raised_exception:
        logging.critical(f"Exception: {raised_exception}")
        raise raised_exception


def publish_alert_on_failure(event: dict, raised_exception: Exception) -> None:
    try:
        sns_client = boto3.client('sns')
        sns_client.publish(
            TopicArn=os.getenv('snsTopicArn'),
            Message=f"Exception: {raised_exception}\nEvent: {event}")
    except Exception as raised_exception:
        logging.critical(f"Exception: {raised_exception}")
        raise raised_exception
