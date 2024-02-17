import unittest
import io
import pandas as pd
from unittest.mock import patch, Mock
from csv_generator import lambda_handler, get_json_obj_from_s3, create_output_template_df, create_output_csv_file

class TestCsvGenerator(unittest.TestCase):

    @patch.dict('os.environ', {'bucket_name': 'test-bucket'})
    def test_lambda_handler_valid_event(self):
        event = {"Input": {"file_date": "010120", "GL_CODE": "some_code"}}  # Sample event
        response = lambda_handler(event, {})
        self.assertEqual(response["status"], "Success")

    def test_lambda_handler_no_event(self):
        with self.assertRaises(Exception):
            lambda_handler(None, {})

    @patch('csv_generator.boto3.client')
    def test_get_json_obj_from_s3(self, mock_boto_client):
        mock_s3 = Mock()
        mock_s3.get_object.return_value = {'Body': io.BytesIO(b'{"key": "value"}')}
        mock_boto_client.return_value = mock_s3

        result = get_json_obj_from_s3('test-bucket', 'test-key/sample.json')
        self.assertIsInstance(result, dict)
        self.assertEqual(result["key"], "value")

    @patch('csv_generator.boto3.client')
    def test_create_output_template_df(self, mock_boto_client):
        mock_s3 = Mock()
        mock_s3.get_object.return_value = {'Body': io.BytesIO(b'test,data\n1,2\n')}
        mock_boto_client.return_value = mock_s3

        result = create_output_template_df('test-bucket', 'test-key/sample.csv')
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]['test'], 1)
        self.assertEqual(result.iloc[0]['data'], 2)

if __name__ == '__main__':
    unittest.main()
