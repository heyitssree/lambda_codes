import unittest
import io
import pandas as pd
from unittest.mock import patch, Mock
from excel_generator import lambda_handler, generate_excel, delete_files, create_df_from_csv_in_s3

class TestExcelGenerator(unittest.TestCase):

    @patch('excel_generator.generate_excel')
    @patch('excel_generator.delete_files')
    @patch.dict('os.environ', {'bucket_name': 'test-bucket', 'csv_file_key': 'test-key'})
    def test_lambda_handler_valid_event(self, mock_delete_files, mock_generate_excel):
        event = [{"Payload": {"file_date": "010120"}}]  # Sample event
        response = lambda_handler(event, {})
        self.assertEqual(response["status"], "success")
        mock_generate_excel.assert_called_once()
        mock_delete_files.assert_called_once()

    def test_lambda_handler_no_event(self):
        with self.assertRaises(OSError):
            lambda_handler(None, {})

    @patch('excel_generator.boto3.client')
    @patch('excel_generator.boto3.resource')
    def test_delete_files(self, mock_boto_resource, mock_boto_client):
        delete_files('test-bucket', 'test-key')
        # Add assertions based on the expected behavior

    @patch('excel_generator.boto3.client')
    def test_create_df_from_csv_in_s3_valid(self, mock_boto_client):
        mock_s3 = Mock()
        mock_s3.get_object.return_value = {'Body': io.BytesIO(b'test,data\n1,2\n')}
        mock_boto_client.return_value = mock_s3

        result = create_df_from_csv_in_s3('test-bucket', 'test-key/sample.csv')
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]['test'], 1)
        self.assertEqual(result.iloc[0]['data'], 2)

if __name__ == '__main__':
    unittest.main()
