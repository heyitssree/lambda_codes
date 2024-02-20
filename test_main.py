import json
import unittest
from unittest import TestCase, mock
from unittest.mock import patch, MagicMock
from main import get_secret, get_private_key, connect_to_snowflake, create_dataframe_from_s3, fetch_schema_from_s3, validate_df_schema, publish_to_sns, connect_to_snowflake, lambda_handler, write_df_to_snowflake, lambda_handler
import pandas as pd
from io import BytesIO

class TestLambdaFunctions(unittest.TestCase):
    
    def setUp(self):
        # Sample DataFrame setup
        self.df = pd.DataFrame({
            'YYYY': [2020], 
            'MM': [1], 
            'DD': [1], 
            'column1': ['value1'], 
            'column2': [123]
        })
        self.table = 'test_table'
        self.database = 'test_database'
        self.schema = 'test_schema'
        # Mock connection setup
        self.mock_conn = mock.Mock()
        self.mock_conn.cursor.return_value.execute = mock.Mock()
        self.mock_conn.cursor.return_value.fetchone = mock.Mock(return_value=True)
        
    @mock.patch('main.boto3.client')
    def test_create_dataframe_from_s3_success(self, mock_boto3_client):
        # Mock the response of `get_object` to mimic S3's behavior
        mock_body = mock.Mock()
        mock_body.read.return_value = b'column1,column2\nvalue1,value2'
        mock_get_object = {'Body': mock_body}
        mock_boto3_client.return_value.get_object.return_value = mock_get_object
        with mock.patch('main.pd.read_excel', return_value=pd.DataFrame({'column1': ['value1'], 'column2': ['value2']})) as mock_read_excel:
            df = create_dataframe_from_s3('bucket-name', 'file-path.xlsx')

            # Assertions to ensure the DataFrame is as expected
            self.assertIsInstance(df, pd.DataFrame)
            self.assertFalse(df.empty)
            mock_read_excel.assert_called_once()
            
    def test_validate_df_schema_success(self):
        # Define a mock DataFrame and schema that should match
        df = pd.DataFrame({'column1': ['value1'], 'column2': [123]})
        schema = {"column1": "object", "column2": "int64"}

        # Validate that the schema validation passes
        result = validate_df_schema(df, schema)
        self.assertTrue(result)

    def test_validate_df_schema_failure(self):
        # Define a mock DataFrame that does not match the expected schema
        df = pd.DataFrame({'column1': ['value1'], 'column3': [123]})
        schema = {"column1": "string", "column2": "int"}

        # Validate that the schema validation fails
        with self.assertRaises(ValueError) as context:
            validate_df_schema(df, schema)

        self.assertTrue("DataFrame does not match schema")
        
    @mock.patch('main.write_pandas')
    @mock.patch('main.logger')
    def test_write_df_to_snowflake_success(self, mock_logger, mock_write_pandas):
        mock_write_pandas.return_value = [True]
        
        # Execute function
        write_df_to_snowflake(self.df, self.table, self.database, self.schema, self.mock_conn)
        
        # Verify SQL delete command was executed correctly
        self.mock_conn.cursor().execute.assert_called_with(
            f" delete from {self.database}.{self.schema}.{self.table} where YYYY={self.df['YYYY'][0]} and MM={self.df['MM'][0]} and DD={self.df['DD'][0]}"
        )
        
        # Verify write_pandas was called with correct parameters
        mock_write_pandas.assert_called_with(
            conn=self.mock_conn, 
            df=self.df, 
            table_name=self.table, 
            schema=self.schema, 
            database=self.database
        )
        
        # Verify logging of successful write
        mock_logger.info.assert_called_with(f"Data written to Snowflake table: {self.table}")

    @mock.patch('main.write_pandas')
    @mock.patch('main.logger')
    def test_write_df_to_snowflake_failure(self, mock_logger, mock_write_pandas):
        mock_write_pandas.return_value = [False, "Error message"]
        
        # Execute function with expectation of failure
        with self.assertRaises(ValueError):
            write_df_to_snowflake(self.df, self.table, self.database, self.schema, self.mock_conn)
        
        # Verify logging of failure
        mock_logger.error.assert_called()

    @patch.dict('os.environ', {
        "SNOWFLAKE_SECRET_ID": "test_secret_id",
        "BUCKET": "test_bucket",
        "SCHEMA_KEY": "test_schema_key",
        "SNOWFLAKE_TABLE": "test_table",
        "SNOWFLAKE_SCHEMA": "test_schema",
        "SNOWFLAKE_DATABASE": "test_database",
        "SNOWFLAKE_WAREHOUSE": "test_warehouse",
        "SNOWFLAKE_ACCOUNT": "test_account",
        "CREATE_TABLE_QUERY_KEY": "test_create_table_query",
        "SNS_ARN": "test_sns_arn"
    })
    @mock.patch('main.get_secret')
    @mock.patch('main.get_private_key')
    @mock.patch('main.connect_to_snowflake')
    @mock.patch('main.create_dataframe_from_s3')
    @mock.patch('main.fetch_schema_from_s3')
    @mock.patch('main.validate_df_schema')
    @mock.patch('main.transform_data')
    @mock.patch('main.create_table_in_snowflake')
    @mock.patch('main.write_df_to_snowflake')
    @mock.patch('main.publish_to_sns')
    @mock.patch('main.logger')
    def test_lambda_handler_success(self, mock_logger, mock_publish_to_sns, mock_write_df_to_snowflake, mock_create_table_in_snowflake, 
                                    mock_transform_data, mock_validate_df_schema, mock_fetch_schema_from_s3, mock_create_dataframe_from_s3, 
                                    mock_connect_to_snowflake, mock_get_private_key, mock_get_secret):
        
        # Setup function mocks
        mock_get_secret.return_value = {"user": "test_user", "privateKey": "test_private_key"}
        mock_get_private_key.return_value = b"mocked_private_key_bytes"
        mock_connect_to_snowflake.return_value = mock.Mock()
        mock_create_dataframe_from_s3.return_value = pd.DataFrame({"test": ["data"]})
        mock_fetch_schema_from_s3.return_value = {"test": "schema"}
        mock_validate_df_schema.return_value = True
        mock_transform_data.return_value = pd.DataFrame({"transformed": ["data"]})
        
        # Mock event and context for lambda_handler invocation
        event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "test_bucket"},
                        "object": {"key": "test_key"}
                    }
                }
            ]
        }
        context = {}
        
        # Execute lambda_handler
        result = lambda_handler(event, context)
        
        # Validate expected outcomes
        self.assertEqual(result, {"status": "Success"})
        # Add more assertions as necessary to verify function behavior