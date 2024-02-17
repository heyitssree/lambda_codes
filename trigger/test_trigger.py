from trigger import lambda_handler

import unittest
from unittest.mock import patch, MagicMock
from trigger import execute_step_function, publish_alert_on_failure, query_execution, query_generator
import os


class TestTriggerLambda(unittest.TestCase):

    @patch('boto3.client')
    def test_query_generator_success(self, mock_boto_client):
        # Mocking the S3 client and its methods
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # Adjusting the mock to emulate the behavior of the actual S3 response
        mock_response_body = MagicMock()
        mock_response_body.read.return_value = b'SELECT * FROM table'
        mock_s3.get_object.return_value = {'Body': mock_response_body}

        # Call the query_generator function
        year, month, day, horizon, query_key, bucket_name = 2023, 8, 18, 1, 'some_query_key', 'some_bucket'
        result_query = query_generator(
            year, month, day, horizon, query_key, bucket_name)

        # Assert based on expected behavior
        self.assertEqual(result_query, 'SELECT * FROM table')

    @patch('boto3.client')
    def test_query_generator_exception(self, mock_boto_client):
        # Mocking the S3 client to raise an exception
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.get_object.side_effect = Exception("Test exception")

        # Call the query_generator function and check for exception handling
        with self.assertRaises(Exception) as context:
            year, month, day, horizon, query_key, bucket_name = 2023, 8, 18, 1, 'some_query_key', 'some_bucket'
            query_generator(year, month, day, horizon, query_key, bucket_name)

        self.assertTrue("Test exception" in str(context.exception))

    @patch('boto3.client')
    def test_query_execution_success(self, mock_boto_client):
        # Mocking the Athena client and its methods
        mock_athena = MagicMock()
        mock_boto_client.return_value = mock_athena
        mock_athena.start_query_execution.return_value = {
            'QueryExecutionId': '1234'}
        mock_athena.get_query_execution.return_value = {
            'QueryExecution': {'Status': {'State': 'SUCCEEDED'}}}

        # Call the query_execution function
        result = query_execution(
            "SELECT * FROM table", "some_bucket", "some_folder")

        # Assert based on expected behavior
        self.assertEqual(result, '1234')

    @patch('boto3.client')
    def test_query_execution_failed(self, mock_boto_client):
        # Mocking the Athena client to simulate a failed query execution
        mock_athena = MagicMock()
        mock_boto_client.return_value = mock_athena
        mock_athena.start_query_execution.return_value = {
            'QueryExecutionId': '1234'}
        mock_athena.get_query_execution.return_value = {
            'QueryExecution': {'Status': {'State': 'FAILED'}}}

        # Call the query_execution function and check for exception handling
        with self.assertRaises(Exception) as context:
            query_execution("SELECT * FROM table",
                            "some_bucket", "some_folder")

        self.assertTrue("Query execution failed" in str(context.exception))

    @patch('boto3.client')
    def test_query_execution_exception(self, mock_boto_client):
        # Mocking the Athena client to raise an exception
        mock_athena = MagicMock()
        mock_boto_client.return_value = mock_athena
        mock_athena.start_query_execution.side_effect = Exception(
            "Test exception")

        # Call the query_execution function and check for exception handling
        with self.assertRaises(Exception) as context:
            query_execution("SELECT * FROM table",
                            "some_bucket", "some_folder")

        self.assertTrue("Test exception" in str(context.exception))

    @patch('boto3.client')
    @patch('os.getenv', return_value='arn:aws:states:...')
    def test_execute_step_function_success(self, mock_getenv, mock_boto_client):
        # Mocking the Step Functions client and its methods
        mock_sf = MagicMock()
        mock_boto_client.return_value = mock_sf
        mock_sf.start_execution.return_value = {
            'executionArn': 'arn:aws:states:...:execution:...', 'startDate': '2023-08-18'}

        # Call the execute_step_function function
        file_details = {"key": "value"}
        response = execute_step_function(file_details)

        # Assert based on expected behavior
        self.assertEqual(response['executionArn'],
                         'arn:aws:states:...:execution:...')

    @patch('boto3.client')
    @patch('os.getenv', return_value='arn:aws:states:...')
    def test_execute_step_function_exception(self, mock_getenv, mock_boto_client):
        # Mocking the Step Functions client to raise an exception
        mock_sf = MagicMock()
        mock_boto_client.return_value = mock_sf
        mock_sf.start_execution.side_effect = Exception("Test exception")

        # Call the execute_step_function function and check for exception handling
        with self.assertRaises(Exception) as context:
            file_details = {"key": "value"}
            execute_step_function(file_details)

        self.assertTrue("Test exception" in str(context.exception))

    @patch('boto3.client')
    @patch('os.getenv', return_value='arn:aws:sns:...')
    def test_publish_alert_success(self, mock_getenv, mock_boto_client):
        # Mocking the SNS client and its methods
        mock_sns = MagicMock()
        mock_boto_client.return_value = mock_sns

        # Call the publish_alert_on_failure function
        event = {"key": "value"}
        exception = Exception("Test exception")
        response = publish_alert_on_failure(event, exception)

        # Assert based on expected behavior - in this case, we just ensure no errors are raised
        self.assertIsNone(response)

    @patch('boto3.client')
    @patch('os.getenv', return_value='arn:aws:sns:...')
    def test_publish_alert_failed(self, mock_getenv, mock_boto_client):
        # Mocking the SNS client to raise an exception
        mock_sns = MagicMock()
        mock_boto_client.return_value = mock_sns
        mock_sns.publish.side_effect = Exception("SNS publish failed")

        # Call the publish_alert_on_failure function and check for exception handling
        with self.assertRaises(Exception) as context:
            event = {"key": "value"}
            exception = Exception("Test exception")
            publish_alert_on_failure(event, exception)

        self.assertTrue("SNS publish failed" in str(context.exception))

    @patch('boto3.client')
    def test_query_execution_cancelled(self, mock_boto_client):
        # Mocking the Athena client and its methods
        mock_athena = MagicMock()
        mock_boto_client.return_value = mock_athena

        # Emulating the behavior of Athena in the CANCELLED state
        mock_athena.get_query_execution.return_value = {
            'QueryExecution': {'Status': {'State': 'CANCELLED'}}}

        # Call the query_execution function
        query_id = "some_query_id"
        query_bucket = "some_bucket"
        database = "some_database"

        with self.assertRaises(Exception) as context:
            response = query_execution(query_id, query_bucket, database)
        self.assertTrue("query execution cancelled" in str(
            context.exception).lower())


if __name__ == '__main__':
    unittest.main()
