import os
import logging
import boto3

logging.getLogger().setLevel(logging.INFO)

def lambda_handler(event: dict, context: dict) -> dict:
    if event:
        logging.info(f"Event: {event}")
        try:
            logging.info("Deleting files")            
        except Exception as error:
            logging.error(error)
            raise error
    else:
        logging.info("No event found")
        
def delete_intermediate_files(file_key: str) -> None:
    try:
        s3_client = boto3.client("s3")
        objects_to_be_delete = s3_client.list_objects(Bucket=os.environ["bucket_name"], Prefix=file_key)
        delete_keys = {"Objects": []}
        delete_keys["Objects"] = [{"Key": k} for k in [obj["Key"] for obj in objects_to_be_delete.get("Contents", [])]]
        s3_client.delete_object(Bucket=os.environ["bucket_name"], Key=delete_keys)
    except Exception as error:
        logging.error(error)
        raise error

def is_file_exist(
    file_key: str
) -> bool:
    """Checks if files exist at the location or not"""
    try:
        s3_client = boto3.client("s3")
        response = s3_client.list_objects_v2(Bucket=os.environ["bucket_name"], Prefix=file_key)
        if 'Contents' in response:
            return True
        else:
            return False
    except Exception as error:
        logging.error(error)
        raise error