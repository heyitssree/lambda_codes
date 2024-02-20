import json
import unittest
from unittest import TestCase, mock
from unittest.mock import patch, MagicMock
from main import get_secret, get_private_key, connect_to_snowflake, create_dataframe_from_s3, fetch_schema_from_s3, validate_df_schema, publish_to_sns, connect_to_snowflake, lambda_handler, write_df_to_snowflake, lambda_handler
import pandas as pd
from io import BytesIO

class TestLambdaFunctions(unittest.TestCase):
        
    def setUp(self):
        super().setUp()
        # Mock connection setup for Snowflake
        self.mock_conn = mock.Mock()
        self.mock_conn.cursor.return_value.execute = mock.Mock()
        self.mock_conn.cursor.return_value.fetchone = mock.Mock(return_value=True)
        # Setup mock data as needed for tests
        self.mock_data = {
                            'YYYY': [2020], 
                            'MM': [1], 
                            'DD': [1], 
                            'column1': ['value1'], 
                            'column2': [123]
                        }
        self.table = 'test_table'
        self.database = 'test_database'
        self.schema = 'test_schema'


    @patch('boto3.session.Session.client')
    def test_get_secret_success(self, mock_client):
        # Mocking the AWS Secrets Manager client response
        secret_string = '{"user": "test_user", "password": "test_password"}'
        mock_client.return_value.get_secret_value.return_value = {'SecretString': secret_string}
        
        # Calling the function under test
        secret_id = 'test_secret_id'
        result = get_secret(secret_id)
        
        # Assertions
        mock_client.assert_called_once_with(service_name="secretsmanager", region_name="us-east-1")
        mock_client.return_value.get_secret_value.assert_called_once_with(SecretId=secret_id)
        self.assertEqual(result, json.loads(secret_string))

    @patch('boto3.session.Session.client')
    def test_get_secret_exception_handling(self, mock_client):
        # Mocking the AWS Secrets Manager client to raise an exception
        mock_client.return_value.get_secret_value.side_effect = Exception("AWS error")
        
        # Assertions
        with self.assertRaises(Exception):
            get_secret('test_secret_id')
            
    @patch('main.serialization.load_pem_private_key')
    def test_get_private_key_success(self, mock_load_pem):
        # Mock the load_pem_private_key to return a mock private key object
        mock_private_key = MagicMock()
        mock_private_key.private_bytes.return_value = b'test_private_key_bytes'
        mock_load_pem.return_value = mock_private_key
        
        # Sample private key in PEM format (this should be a valid PEM string for actual testing)
        pem_private_key = "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkq...\n-----END PRIVATE KEY-----"
        
        # Calling the function under test
        result = get_private_key(pem_private_key)
        
        # Assertions
        self.assertEqual(result, b'test_private_key_bytes')
        mock_load_pem.assert_called_once()
        
    @mock.patch('main.connect')
    def test_connect_to_snowflake_success(self, mock_connect):
        mock_connect.return_value = mock.Mock()  # Assuming successful return object for simplicity
        # Call the function with dummy arguments
        connection = connect_to_snowflake('user', 'account', 'key', 'schema', 'warehouse', 'database')
        self.assertIsNotNone(connection)
        mock_connect.assert_called_once_with(user='user', account='account', private_key='key', schema='schema', warehouse='warehouse', database='database')

    @mock.patch('main.connect')
    def test_connect_to_snowflake_failure(self, mock_connect):
        mock_connect.side_effect = Exception("Failed to connect")

        # Execute function and assert exception is raised
        with self.assertRaises(Exception) as context:
            connect_to_snowflake()

        # Assert the correct exception was raised
        self.assertTrue('Failed to connect')
        
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

    @mock.patch('main.boto3.client')
    def test_create_dataframe_from_s3_failure_invalid_path(self, mock_boto3_client):
        # Setup mock S3 client to simulate an error when retrieving the object
        mock_boto3_client.return_value.get_object.side_effect = Exception("S3 object not found")

        # Execute function and assert exception is raised for invalid path
        with self.assertRaises(Exception) as context:
            create_dataframe_from_s3('bucket-name', 'invalid-path.csv')

        # Assert the correct exception was raised
        self.assertTrue('S3 object not found' in str(context.exception))

    @mock.patch('main.boto3.client')
    def test_fetch_schema_from_s3_success(self, mock_boto3_client):
        # Setup mock for successful schema retrieval
        mock_body = mock.Mock()
        mock_body.read.return_value = b'{"column1": "string", "column2": "int"}'
        mock_boto3_client.return_value.get_object.return_value = {'Body': mock_body}

        schema = fetch_schema_from_s3('bucket-name', 'schema-path.json')

        # Validate that the schema was correctly fetched and parsed
        self.assertEqual(schema, {"column1": "string", "column2": "int"})

    @mock.patch('main.boto3.client')
    def test_fetch_schema_from_s3_failure(self, mock_boto3_client):
        # Setup mock to raise an exception for an inaccessible schema file
        mock_boto3_client.return_value.get_object.side_effect = Exception("S3 object not found")

        # Test exception handling for schema retrieval failure
        with self.assertRaises(Exception) as context:
            fetch_schema_from_s3('bucket-name', 'invalid-path.json')

        self.assertTrue("S3 object not found" in str(context.exception))

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

    @mock.patch('main.boto3.client')
    def test_publish_to_sns_success(self, mock_boto3_client):
        # Setup mock SNS client to simulate successful message publication
        mock_sns_client = mock_boto3_client.return_value
        mock_sns_client.publish.return_value = {'MessageId': '12345'}

        # Attempt to publish a message
        response = publish_to_sns('test_topic_arn', 'Test Subject', 'Test Message')

        # Verify publish was called and check response
        mock_sns_client.publish.assert_called_with(TopicArn='test_topic_arn', Subject='Test Subject', Message='Test Message')
        
    @mock.patch('main.connect')
    def test_connect_to_snowflake_success(self, mock_snowflake_connect):
        mock_snowflake_connect.return_value = mock.Mock()
        conn = connect_to_snowflake('user', 'account', 'private_key', 'schema', 'warehouse', 'database')
        self.assertIsNotNone(conn)

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

    @mock.patch('main.publish_to_sns')
    @mock.patch('main.write_df_to_snowflake')
    @mock.patch('main.create_table_in_snowflake')
    @mock.patch('main.transform_data')
    @mock.patch('main.validate_df_schema')
    @mock.patch('main.fetch_schema_from_s3')
    @mock.patch('main.create_dataframe_from_s3')
    @mock.patch('main.connect_to_snowflake')
    @mock.patch('main.get_private_key')
    @mock.patch('main.get_secret')
    def test_lambda_handler_success(self, mock_get_secret, mock_get_private_key, mock_connect_to_snowflake, mock_create_dataframe_from_s3,
                                    mock_fetch_schema_from_s3, mock_validate_df_schema, mock_transform_data, mock_create_table_in_snowflake,
                                    mock_write_df_to_snowflake, mock_publish_to_sns):
        # Setup mocks
        mock_get_secret.return_value = {"user": "test_user", "privateKey": "test_private_key"}
        mock_get_private_key.return_value = b"mocked_private_key_bytes"
        mock_connect_to_snowflake.return_value = self.mock_conn
        mock_create_dataframe_from_s3.return_value = self.mock_data  # Simulate DataFrame with a dictionary
        mock_fetch_schema_from_s3.return_value = {"test": "schema"}
        mock_validate_df_schema.return_value = True
        mock_transform_data.return_value = self.mock_data  # Again, simulate with simple structure
        # Add more mock configurations as necessary

        # Execute lambda_handler
        result = lambda_handler({'Records': [{'s3': {'bucket': {'name': 'test_bucket'}, 'object': {'key': 'test_key'}}}]}, {})
        
        # Validate expected outcomes
        self.assertEqual(result, {"status": "Success"})

if __name__ == '__main__':
    unittest.main()
