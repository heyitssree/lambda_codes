import io
import os
import json
import logging
import boto3
import pandas as pd
from typing import List

logging.getLogger().setLevel(logging.INFO)

def boto3_client(service_name, *args, **kwargs):
    return boto3.client(service_name, *args, **kwargs)

def create_df_from_csv_in_s3(bucket_name, file_key):
    s3_client = boto3_client('s3')
    csv_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    data = csv_obj["Body"].read()
    df = pd.read_csv(StringIO(data))
    return df

def fetch_data_from_s3(bucket_name, file_keys):
    input_dfs = [create_df_from_csv_in_s3(bucket_name, key) for key in file_keys]
    input_df = input_dfs[0]
    for df in input_dfs[1:]:
        input_df = pd.merge(input_df, df, on=["GL Code", "ALFA_ID"], how="left")
    return input_df

def process_data(input_df, bucket_name, file_date):
    gl_codes = pd.unique(input_df["GL Code"]).tolist()
    records = []
    for gl_code in gl_codes:
        records.append(
            create_seg_values_for_gl_code(
                input_df=input_df,
                gl_code=gl_code,
                file_date=file_date,
                bucket_name=bucket_name
            )
        )
    return records

def refactored_lambda_handler(event, _context):
    if not event:
        logging.error("Empty event received")
        return {"Records": []}
    
    file_date = event.get("file_date", "")
    bucket_name = event.get("bucket_name", "")
    file_keys = event.get("key", [])
    
    input_df = fetch_data_from_s3(bucket_name, file_keys)
    records = process_data(input_df, bucket_name, file_date)
    
    return {"Records": records}