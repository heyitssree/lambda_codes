"""Lambda for generating URL."""
import os
import logging
import boto3
from botocore.client import Config

logging.getLogger().setLevel(logging.INFO)

def lambda_handler(event: dict, context: dict) -> dict:
    if event:
        logging.info("This is the event we received: %s", event)
        try:
            file_date = event["valDate"]
            file_name = "Purchassetspreads.xlsx"
            file_key = f"processed/yyy={file_date[:4]}/mm={file_date[5:7]}/dd={file_date[8:]}/{file_name}"
            logging.info("This is the file key: %s", file_key)
            s3_client = boto3.client("s3", config=Config(signature_version="s3v4"))
            
            response = s3_client.list_objects_v2(
                Bucket=os.environ["bucket_name"],
                Prefix=file_key,
            )
            if 'Contents' in response:
                url = s3_client.generate_presigned_url(
                    ClientMethod="get_object",
                    Params={"Bucket": os.environ["bucket_name"], "Key": file_key},
                    ExpiresIn=900   
                )
                
                return {"statusCode": 200, "Url": url}
            else:
                logging.error("Couldn't find %s", file_key.rsplit('/', 1)[-1])
                raise OSError("Couldn't find file")
        except Exception as error:
            logging.error("Error: %s", error)
            raise error
        
    else:
        logging.error("No event received")
        raise OSError("No event received")