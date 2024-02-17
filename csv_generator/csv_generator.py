"""Run as a Map state to generate csv files for all TAA segments."""
import os, io, json, logging, time, boto3, pandas as pd

logging.getLogger().setLevel(logging.INFO)

def lambda_handler(
    event: dict,
    context: dict
) -> dict:
    """Lambda handler."""
    if event:
        logging.info(f"Event: {event}")
        event = event["Input"]
        file_date = event["file_date"]
        gl_code = event["GL_CODE"]
        
        try:
            
            return {
                "status": "Success"
            }
        except Exception as error:
            logging.error(error)
            raise error
    else:
        logging.error("No event found.")
        raise Exception("No event found.")

def get_json_obj_from_s3(
    bucket_name: str,
    key: str
) -> str:
    """Method to get a json object from s3."""
    try:
        s3 = boto3.client("s3")
        file_obj = s3.get_object(Bucket=bucket_name, Key=key)
        file_content = file_obj["Body"].read()
        return json.loads(file_content)
    except Exception as error:
        logging.error(error)
        raise error

def create_output_template_df(
    bucket_name: str,
    file_key: str
) -> pd.DataFrame:
    """Method to create a Dataframe from a template file in S3 location."""
    try:
        s3 = boto3.client("s3")
        file_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        file_content = file_obj["Body"].read()
        return pd.read_csv(io.BytesIO(file_content))
    except Exception as error:
        logging.error(error)
        raise error

def create_output_csv_file(
    template_df: pd.DataFrame,
    values: list,
    file_name: str,
    file_date: str
) -> None:
    """Method to create the required output CSV files."""
    try:
        start = time.time()
        transposed = template_df.T
        for i, value in enumerate(values[0]):
            transposed[i + 3].fillna(value, inplace=True, limit=12)
        for i, value in enumerate(values[1]):
            transposed[i + 3].fillna(value, inplace=True, limit=12)
        for i, value in enumerate(values[2]):
            transposed[i + 3].fillna(value, inplace=True)
            
        transposed.T.to_csv(
            f"s3://{os.eniron['bucket_name']}/processing/temp_csv_files/"
            f"yyyy=20{file_date[4:]}/mm={file_date[0:2]}/dd={file_date[2:4]}/{file_name}.csv",
            index=False
        )
        end = time.time()
        total_time = end - start
        logging.info(f"Time taken to create {file_name}.csv: {total_time}")
        
    except Exception as error:
        logging.error(error)
        raise error