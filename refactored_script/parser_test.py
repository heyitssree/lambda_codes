import unittest
from unittest.mock import patch, Mock
import pandas as pd
from io import StringIO
from parser import fetch_data_from_s3, process_data, refactored_lambda_handler

def mock_boto3_client(service_name, *args, **kwargs):
    assert service_name == "s3"
    class MockS3Client:
        def get_object(self, Bucket, Key):
            csv_data = "GL Code,ALFA_ID,value1,value2\\n1,100,10,20\\n2,200,15,25\\n"
            return {{"Body": StringIO(csv_data)}}
    return MockS3Client()

def mock_create_seg_values_for_gl_code(input_df, gl_code, file_date, bucket_name):
    return {{"File_date": file_date, "GL_Code": gl_code, "data_path": "mock_path"}}

class TestRefactoredFunctions(unittest.TestCase):
    
    @patch('__main__.boto3_client', side_effect=mock_boto3_client)
    def test_fetch_data_from_s3(self, mock_client):
        df = fetch_data_from_s3(bucket_name="test_bucket", file_keys=["test_key1.csv", "test_key2.csv"])
        expected_df = pd.DataFrame({{"GL Code": [1, 2], "ALFA_ID": [100, 200], "value1_x": [10, 15], "value2_x": [20, 25], "value1_y": [10, 15], "value2_y": [20, 25]}})
        pd.testing.assert_frame_equal(df, expected_df)

    @patch('__main__.create_seg_values_for_gl_code', side_effect=mock_create_seg_values_for_gl_code)
    def test_process_data(self, mock_function):
        input_df = pd.DataFrame({{"GL Code": [1, 2], "ALFA_ID": [100, 200], "value1": [10, 15], "value2": [20, 25]}})
        records = process_data(input_df, bucket_name="test_bucket", file_date="2023-08-17")
        expected_records = [{{"File_date": "2023-08-17", "GL_Code": 1, "data_path": "mock_path"}}, {{"File_date": "2023-08-17", "GL_Code": 2, "data_path": "mock_path"}}]
        self.assertEqual(records, expected_records)
    
    @patch('__main__.boto3_client', side_effect=mock_boto3_client)
    @patch('__main__.create_seg_values_for_gl_code', side_effect=mock_create_seg_values_for_gl_code)
    def test_refactored_lambda_handler(self, mock_seg_function, mock_s3_function):
        event = {{"file_date": "2023-08-17", "bucket_name": "test_bucket", "key": ["test_key1.csv", "test_key2.csv"]}}
        result = refactored_lambda_handler(event, None)
        expected_result = {{"Records": [{{"File_date": "2023-08-17", "GL_Code": 1, "data_path": "mock_path"}}, {{"File_date": "2023-08-17", "GL_Code": 2, "data_path": "mock_path"}}]}}
        self.assertEqual(result, expected_result)

if __name__ == '__main__':
    unittest.main()