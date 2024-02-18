
import logging
import os
import json
import boto3
import pandas as pd
from io import BytesIO
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Decorators for logging
def log_methods_non_sensitive(func):
    def wrapper(*args, **kwargs):
        logger.info(f"Calling {func.__name__} with args: {args} and kwargs: {kwargs}")
        return func(*args, **kwargs)
    return wrapper

def log_method_sensitive(func):
    def wrapper(*args, **kwargs):
        logger.info(f"Method: {func.__name__} called.")
        return func(*args, **kwargs)
    return wrapper

# AWS Secrets Manager interaction
def get_secret(secret_id: str):
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name="us-east-1")
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_id)
        secret_json_string = get_secret_value_response["SecretString"]
        return json.loads(secret_json_string)
    except Exception as e:
        logger.error(f"Error getting secret: {e}")
        raise

# Cryptography operations
def get_private_key(snowflake_private_key):
    try:
        p_key = bytes(snowflake_private_key, "utf-8")
        p_key = serialization.load_pem_private_key(p_key, password=None, backend=default_backend())
        return p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
    except Exception as e:
        logger.error(f"Error loading private key: {e}")
        raise

# Snowflake connection
def connect_to_snowflake(snowflake_user, snowflake_account, private_key, snowflake_schema, snowflake_warehouse, snowflake_database):
    try:
        connection = connect(
            user=snowflake_user,
            account=snowflake_account,
            private_key=private_key,
            schema=snowflake_schema,
            warehouse=snowflake_warehouse,
            database=snowflake_database
        )
        return connection
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {e}")
        raise

# Data processing functions
def create_dataframe_from_s3(bucket, key):
    try:
        s3 = boto3.client("s3")
        file_obj = s3.get_object(Bucket=bucket, Key=key)
        data = BytesIO(file_obj["Body"].read())
        df = pd.read_excel(data)
        df.reset_index(drop=True, inplace=True)
        return df
    except Exception as error:
        logger.error(error)
        raise error
    
def fetch_schema_from_s3(bucket, key):
    try:
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=bucket, Key=key)
        schema = json.load(BytesIO(obj["Body"].read()))
        return schema
    except Exception as error:
        logger.error(error)
        raise error

def validate_df_schema(df, schema):
    try:
        df_types = df.dtypes.apply(lambda x: x.name).to_dict()
        
        missing_columns = [col for col in schema if col not in df_types]
        new_columns = [col for col in df_types if col not in schema]
        
        if missing_columns:
            raise ValueError(f"Missing columns: {missing_columns}")
        if new_columns:
            raise ValueError(f"New columns: {new_columns}")
        if df_types != schema:
            raise ValueError(f"Schema mismatch: {df_types}")
        if list(df.columns) != list(schema.keys()):
            raise ValueError("Column order mismatch")
        
        return True
    except Exception as error:
        logger.error(error)
        raise error

def transform_data(df: pd.DataFrame):
    try:
        df.columns = [col.upper().replace(' (%)', '_PCT').replace(' ($)', '').replace(' ', '_') for col in df.columns]
        df['YYYY'] = pd.to_datetime('today').year
        df['MM'] = pd.to_datetime('today').month
        df['DD'] = pd.to_datetime('today').day
        df = df[~df.iloc[:, 0].str.contais('Total', na=False)]
        return df
    except Exception as error:
        logger.error(error)
        raise error
    
def create_table_in_snowflake(conn, bucket, table_ddl_key, database, schema, table):
    try:
        s3 = boto3.client('s3')
        
        obj = s3.get_object(Bucket=bucket, Key=table_ddl_key)
        
        script = obj['Body'].read().decode('utf-8')
        
        script = script.replace('@database', database).replace('@schema', schema).replace('@table', table)
        
        logger.info(f"Script fetched from S3: {script}")
        
        cur = conn.cursor()
        
        cur.execute(script)
        
        logger.info(f"Table created in Snowflake: {table}")
        
    except Exception as error:
        logger.error(error)
        raise error
    
def write_df_to_snowflake(df, table, database, schema, conn):
    try:
        cur = conn.cursor()
        
        logger.info(f"Writing to Snowflake table: {table}")
        cur.execute(f" delete from {database}.{schema}.{table} where YYYY={df['YYYY'][0]} and MM={df['MM'][0]} and DD={df['DD'][0]}")
        
        response = write_pandas(conn=conn, df=df, table_name=table, schema=schema, database=database)
        
        if response[0]==True:
            logger.info(f"Data written to Snowflake table: {table}")
        else:
            raise ValueError(f"Error writing to Snowflake table: {table}")
    except Exception as error:
        logger.error(error)
        raise error
    
def publish_to_sns(sns_arn, subject, message):
    try:
        sns = boto3.client('sns')
        sns.publish(TopicArn=sns_arn, Subject=subject, Message=message)
        logger.info(f"Published to SNS: {subject}")
    except Exception as error:
        logger.error(error)
        raise error
    
def lambda_handler(event, context):
    if event:
        logger.info(f"Event: {event}")
        try:
            logger.info(f"Envrinment variables: {os.environ}")
            
            snowflake_secret_id = os.environ["SNOWFLAKE_SECRET_ID"]
            bucket= os.environ["BUCKET"]
            schema_key = os.environ["SCHEMA_KEY"]
            snowflake_table = os.environ["SNOWFLAKE_TABLE"]
            snowflake_schema = os.environ["SNOWFLAKE_SCHEMA"]
            snowflake_database = os.environ["SNOWFLAKE_DATABASE"]
            snowflake_warehouse = os.environ["SNOWFLAKE_WAREHOUSE"]
            snowflake_account = os.environ["SNOWFLAKE_ACCOUNT"]
            create_table_query_key = os.environ["CREATE_TABLE_QUERY_KEY"]
            
            # Getting snowflake credentials
            snowflake_credentials = get_secret(snowflake_secret_id)
            snowflake_user = snowflake_credentials["user"]
            snowflake_private_key = get_private_key(snowflake_credentials["privateKey"])
            
            # Connecting to snowflake
            conn = connect_to_snowflake(snowflake_user, snowflake_account, snowflake_private_key, snowflake_schema, snowflake_warehouse, snowflake_database)
            
            # Fetching details from event
            event_bucket = event['Records'][0]['s3']['bucket']['name']
            event_key = event['Records'][0]['s3']['object']['key']
            
            df = create_dataframe_from_s3(event_bucket, event_key)
            
            if validate_df_schema(df, fetch_schema_from_s3(bucket, schema_key)):
                df = transform_data(df)
                create_table_in_snowflake(conn, bucket, create_table_query_key, snowflake_database, snowflake_schema, snowflake_table)
                write_df_to_snowflake(df, snowflake_table, snowflake_database, snowflake_schema, conn)
                publish_to_sns(os.environ["SNS_ARN"], "Success", f"Data loaded to Snowflake table: {snowflake_table}")
                
                return {
                    "status": "Success"
                }
            else:
                logger.info("Schema validation failed.")
                raise ValueError("Schema validation failed.")
        except Exception as e:
            logger.error(f"Error in lambda handler: {e}")
            raise
    else:
        logger.error("No event found.")
        raise ValueError("No event found.")