"""Parses the input file and creates multiple events for multiple TAA Segments."""
import io
import os
import json
import logging
from typing import List
import boto3
import pandas as pd
import numpy as np

logging.getLogger().setLevel(logging.INFO)


def lambda_handler(
    event: dict,
    _context: dict
) -> dict:
    """Main function for Parser Lambda."""
    output_dict = {
        "Records": []
    }

    if event:
        try:
            logging.info("Event: {}".format(event))
            file_date = event["file_date"]
            logging.info("File Date: {}".format(file_date))
            
            input_df_1 = create_df_from_csv_in_s3(
                bucket_name=event["bucket_name"],
                file_key=event["key"][0]
            )
            input_df_2 = create_df_from_csv_in_s3(
                bucket_name=event["bucket_name"],
                file_key=event["key"][1]
            )
            input_df_3 = create_df_from_csv_in_s3(
                bucket_name=event["bucket_name"],
                file_key=event["key"][2]
            )
            
            input_df = pd.merge(input_df_1, input_df_2, on=["GL Code", "ALFA_ID"], how="left")
            input_df = pd.merge(input_df, input_df_3, on=["GL Code", "ALFA_ID"], how="left")
            
            gl_codes = pd.unique(input_df["GL Code"]).tolist()
            alfa_ids = pd.unique(input_df["ALFA_ID"]).tolist()
            table_ppt_df = create_df_from_csv_in_s3(
                bucket_name=event["bucket_name"],
                file_key=os.environ["table_ppt_key"]
            )
            for gl_code in gl_codes:
                logging.info("Creating event for gl_code %s:", gl_code)
                output_dict["Records"].append(
                    create_seg_values_for_gl_code(
                        input_df=input_df,
                        gl_code=gl_code,
                        file_date=file_date,
                        bucket_name=event["bucket_name"]
                    )
                )
                table_ppt_df = create_df_for_table_ppt(
                    input_df=table_ppt_df,
                    gl_code=gl_code
                )
                logging.info("Creating Table Properties Tab for the Output")
                table_ppt_df.to_csv(
                    f"s3://{event['bucket_name']}/processing/Temp_csv_files/",
                    f"yyyy=20{file_date[4:]}/mm={file_date[0:2]}/dd={file_date[2:4]}/TableProperties.csv",
                    index=False
                )
                
            logging.info("Dynamically creating a template file to be used in the next step")
            template = create_edited_template(
                alfa_ids=alfa_ids,
                bucket_name=event["bucket_name"]
            )
            if template:
                logging.info("Template file created successfully")
            else:
                logging.error("Error in creating template file")
                raise OSError("Error in creating template file")
            response = {
                "Status": "Success",
                "Output": output_dict
            }
            logging.info("Output: {}".format(response))
            return response            

        except Exception as error:
            logging.error("Error: {}".format(error))
            raise error

    else:
        logging.error("No event found.")
        raise OSError("No event found.")


def create_df_from_csv_in_s3(
    bucket_name: str,
    file_key: str
) -> pd.DataFrame:
    """Creates a DataFrame from a CSV file in S3."""
    try:
        logging.info("Creating DataFrame from CSV in S3.")
        s3 = boto3.client("s3")
        file_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        file_contents = file_obj["Body"].read()
        read_data = io.BytesIO(file_contents)
        return pd.read_csv(read_data)
    except Exception as error:
        logging.error("Error: {}".format(error))
        raise error


def create_seg_values_for_gl_code(
    input_df: pd.DataFrame,
    gl_code: int,
    file_date: str,
    bucket_name: str
) -> str:
    try:
        """Method to return all fial dollar values of a gl_code as list."""
        s3 = boto3.client("s3")
        filtered_input_df = input_df[input_df["GL Code"] == gl_code]
        values_h1 = filtered_input_df["FinalDollars_h1"].values.tolist()
        values_h2 = filtered_input_df["FinalDollars_h2"].values.tolist()
        values_h3 = filtered_input_df["FinalDollars_h3"].values.tolist()
        logging.info("%s has %s differnet final dollars in H1",
                     gl_code, len(values_h1))
        logging.info("%s has %s differnet final dollars in H2",
                     gl_code, len(values_h2))
        logging.info("%s has %s differnet final dollars in H3",
                     gl_code, len(values_h3))
        output_dict = {
            "values_h1": values_h1,
            "values_h2": values_h2,
            "values_h3": values_h3
        }
        uploadByteStream = io.BytesIO(json.dumps(output_dict).encode("UTF-8"))
        path = f"processing/gl_codes/{gl_code}/data.json"
        s3.put_object(Bucket=bucket_name, Key=path, Body=uploadByteStream)
        return {
            "File_date": file_date,
            "GL_Code": gl_code,
            "data_path": path
        }
    except Exception as error:
        logging.error("Error: {}".format(error))
        raise error

def create_df_for_table_ppt(
    input_df: pd.DataFrame,
    gl_code: int
) -> pd.DataFrame:
    """Method to return a dataframe for table ppt."""
    try:
        data = {
            "Name": f"InvestPctSeg{gl_code}",
            "SheetName": f"InvestPctSeg{gl_code}",
            "ModuleGroup": "Asset",
            "IndexType": "ProjectionYearAndMonth",
            "DataType": "Real",
            "Description": "TAA/SAA for Invest%",
            "DisplayWidth": 12,
            "DisplayDecimals": 10
        }
        data_df = pd.DataFrame.from_records([data], index=['indexLabel'])
        input_df = pd.concat([input_df, data_df], ignore_index=True)
        return input_df
    except Exception as error:
        logging.error("Error: {}".format(error))
        raise error
    
def create_edited_template(
    alfa_ids: List,
    bucket_name: str
) -> bool:
    """Method to create new edited template files."""
    try:
        template_df = create_df_from_csv_in_s3(
            bucket_name=bucket_name,
            file_key="template_files/PurchTempalte.csv"
        )
        to_append = pd.DataFrame(template_df, index=[3])
        for alfa_id in alfa_ids:
            to_append["ck.Cusip"] = to_append["ck.Cusip"].fillna(alfa_id)
            template_df = pd.concat([template_df, to_append], ignore_index=True)
            to_append['ck.Cusip'] = to_append['ck.Cusip'].replace(alfa_id, np.nan, inplace=True)
            
        template_df = template_df.drop(labels=3, axis=0)
        template_df.to_csv(
            f"s3://{bucket_name}/processing/PurchTempalte.csv",
            index=False
        )
        return True
    except Exception as error:
        logging.error("Error: {}".format(error))
        raise error