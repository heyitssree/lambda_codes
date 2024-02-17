"""Generates single excel file by appending multiple CSV files as different sheets."""
import io, os, logging, boto3, pandas as pd

logging.getLogger().setLevel(logging.INFO)


def lambda_handler(
    event: dict,
    context: dict
) -> dict:
    if event:
        try:
            logging.info(f"Event: {event}")
            file_date = event[0]["Payload"]["file_date"]
            edit_file_name = f"20{file_date[4:]}-{file_date[0:2]}-{file_date[2:4]}"
            generate_excel(
                filenames = os.environ['bucket_name'],
                bucket_name = os.environ['bucket_name'],
                file_key = os.environ['csv_file_key']
            )
            logging.info("Deleting files from S3.")
            delete_files(
                bucket_name = os.environ['bucket_name'],
                file_key = os.environ['csv_file_key']
            )
            logging.info("Excel file generated successfully.")
            return {
                "status": "success",
                "message": "Excel file generated successfully"
            }
        except Exception as error:
            logging.error(f"Error: {error}")
            raise error
    else:
        logging.error("No event found.")
        raise OSError("No event found.")
    
def create_df_from_csv_in_s3(
    bucket_name: str,
    file_key: str
) -> pd.DataFrame:
    """Method to create a Pandas Dataframe from a csv in s3 location."""
    try:
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        return pd.read_csv(io.BytesIO(obj['Body'].read()))
    except Exception as error:
        logging.error(f"Error: {error}")
        raise error

def generate_excel(
    filenames: list,
    bucket_name: str,
    file_key: str
) -> None:
    """Method to generate a consolidated excel file for all segments."""
    s3 = boto3.client('s3')
    file_date = filenames[0]["Payload"]["file_date"]
    edited_file_key = f"yyyy=20{file_date[4:]}-mm={file_date[0:2]}-dd={file_date[2:4]}"
    writer = pd.ExcelWriter(f"/tmp/Purchassetspreads.xslx", engine='xlsxwriter')
    logging.info("Addingtable properties to sheet excel.")
    table_ppt_df = create_df_from_csv_in_s3(
        bucket_name = bucket_name,
        file_key = f"{file_key}/{edited_file_key}/TableProperties.csv"
    )
    table_ppt_df.to_excel(writer, sheet_name='TableProperties', index=False)
    for filename in filenames:
        logging.info(f"Adding {filename} to sheet excel.")
        file = filename["Payload"]["file_name"]
        data_frame = create_df_from_csv_in_s3(
            bucket_name = bucket_name,
            file_key = f"{file_key}/{edited_file_key}/{file}"
        )
        data_frame.to_excel(writer, sheet_name=file, index=False)
    writer.close()
    s3.upload_file(
        Filename = f"/tmp/Purchassetspreads.xslx",
        Bucket = bucket_name,
        Key = f"processed/{edited_file_key}/Purchassetspreads.xslx"
    )
    
def delete_files(bucket_name: str, file_key: str) -> None:
    """Method to delete files from S3."""
    try:
        s3_client = boto3.client('s3')
        s3_resource = boto3.resource('s3')
        bucket = s3_resource.Bucket(bucket_name)
        for obj in bucket.objects.filter(Prefix=file_key):
            path, file = os.path.split(obj.key)
            s3_client.delete_object(Bucket=bucket_name, Key=obj.key)
    except Exception as error:
        logging.error(f"Error: {error}")
        raise error        
    