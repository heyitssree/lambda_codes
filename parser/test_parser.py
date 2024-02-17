import unittest
import io
from unittest.mock import patch, MagicMock, Mock
import pandas as pd
import numpy as np
from parser import create_df_for_table_ppt, create_df_from_csv_in_s3, create_edited_template, create_seg_values_for_gl_code, lambda_handler

class TestParserLambda(unittest.TestCase):
    
    
    def setUp(self):
        self.mock_event = {
            "file_date": "2023-08-18",
            "bucket_name": "mock_bucket",
            "key": ["mock_key_1", "mock_key_2", "mock_key_3"]
        }
        self.mock_df = pd.DataFrame({
            "GL Code": [123, 123, 456],
            "ALFA_ID": ["A", "B", "C"],
            "FinalDollars_h1": [10, 20, 30],
            "FinalDollars_h2": [15, 25, 35],
            "FinalDollars_h3": [20, 30, 40]
        })
        self.mock_output_record = {
            "File_date": "2023-08-18",
            "GL_Code": 123,
            "data_path": "processing/gl_codes/123/data.json"
        }
        self.mock_table_ppt_df = pd.DataFrame({
            "Name": ["mock_name"],
            "SheetName": ["mock_sheet"],
            "ModuleGroup": ["mock_module"],
            "IndexType": ["mock_index"],
            "DataType": ["mock_data"],
            "Description": ["mock_desc"],
            "DisplayWidth": [10],
            "DisplayDecimals": [2]
        })
        self.mock_template = True

    @patch('boto3.client')
    def test_create_df_from_csv_in_s3_success(self, mock_boto_client):
        # Mocking the S3 client and its methods for successful data retrieval
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # Adjusting mock to make 'Body' attribute behave like a file object and return bytes
        mock_file_object = Mock()
        mock_file_object.read.return_value = b"col1,col2\nvalue1,value2"
        mock_s3.get_object.return_value = {'Body': mock_file_object}

        # Call the function
        df = create_df_from_csv_in_s3('mock_bucket', 'mock_key')

        # Assert that the returned object is a DataFrame and contains expected data
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape, (1, 2))
        self.assertTrue("col1" in df.columns)
        self.assertTrue("col2" in df.columns)

    @patch('boto3.client')
    def test_create_df_from_csv_in_s3_s3_failure(self, mock_boto_client):
        # Mocking the S3 client to raise an exception
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.get_object.side_effect = Exception("S3 retrieval failed")

        # Call the function and check for exception handling
        with self.assertRaises(Exception) as context:
            create_df_from_csv_in_s3('mock_bucket', 'mock_key')
        
        self.assertTrue("S3 retrieval failed" in str(context.exception))

    @patch('boto3.client')
    def test_create_df_from_csv_in_s3_invalid_csv(self, mock_boto_client):
        # Mocking the S3 client to return invalid CSV data
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.get_object.return_value = {'Body': "invalid data"}

        # Call the function and check for exception handling
        with self.assertRaises(Exception):
            create_df_from_csv_in_s3('mock_bucket', 'mock_key')

    def test_create_df_for_table_ppt_success(self):
        # Mock input DataFrame
        mock_df = pd.DataFrame({
            'Name': ['TestName'],
            'SheetName': ['TestSheet'],
            'ModuleGroup': ['TestGroup'],
            'IndexType': ['TestIndex'],
            'DataType': ['TestType'],
            'Description': ['TestDesc'],
            "DisplayWidth": [12],
            'DisplayDecimals': [10]
        })

        # Expected output DataFrame after adding new row
        gl_code = 123
        new_data = {
            "Name": f"InvestPctSeg{gl_code}",
            "SheetName": f"InvestPctSeg{gl_code}",
            "ModuleGroup": "Asset",
            "IndexType": "ProjectionYearAndMonth",
            "DataType": "Real",
            "Description": "TAA/SAA for Invest%",
            "DisplayWidth": 12,
            "DisplayDecimals": 10
        }
        expected_output = pd.concat([mock_df, pd.DataFrame([new_data])], ignore_index=True)

        result_df = create_df_for_table_ppt(mock_df, gl_code)

        # Check if the returned DataFrame matches the expected output
        pd.testing.assert_frame_equal(result_df, expected_output)

    @patch.object(pd, 'concat', side_effect=Exception("Concatenation Error"))
    def test_create_df_for_table_ppt_failure(self, _):
        mock_df = pd.DataFrame({'Name': ['Test']})

        with self.assertRaises(Exception) as context:
            create_df_for_table_ppt(mock_df, 123)

        self.assertIn("Concatenation Error", str(context.exception))

    @patch.object(pd.DataFrame, 'to_csv')
    @patch.object(pd, 'concat')
    @patch('parser.boto3.client')
    def test_create_edited_template_success(self, mock_boto_client, mock_concat, mock_to_csv):
        mock_s3 = Mock()
        mock_s3.get_object.return_value = {'Body': io.BytesIO(b'ck.Cusip\n\n\n')}
        mock_boto_client.return_value = mock_s3

        mock_concat.return_value = pd.DataFrame({
            'ck.Cusip': [None, None, None],
        }, index=[1, 2, 3])

        result = create_edited_template(['alfa1', 'alfa2'], 'mock_bucket')

        self.assertTrue(result)
        mock_to_csv.assert_called_once()

    @patch('parser.boto3.client')
    def test_create_edited_template_s3_read_failure(self, mock_boto_client):
        mock_s3 = Mock()
        mock_s3.get_object.side_effect = Exception("S3 read failure")
        mock_boto_client.return_value = mock_s3

        with self.assertRaises(Exception) as context:
            create_edited_template(['alfa1', 'alfa2'], 'mock_bucket')

        self.assertIn("S3 read failure", str(context.exception))
        
    @patch.object(pd, 'concat')
    @patch('parser.boto3.client')
    def test_create_edited_template_excel_modification_failure(self, mock_boto_client, mock_concat):
        mock_s3 = Mock()
        mock_s3.get_object.return_value = {'Body': io.BytesIO(b'ck.Cusip\n')}
        mock_boto_client.return_value = mock_s3

        mock_concat.side_effect = Exception("Excel modification failure")

        with self.assertRaises(Exception) as context:
            create_edited_template(['alfa1', 'alfa2'], 'mock_bucket')

        self.assertIn("Excel modification failure", str(context.exception))

    @patch("boto3.client")
    def test_create_seg_values_for_gl_code_success(self, mock_boto3_client):
        mock_s3 = Mock()
        mock_s3.put_object.return_value = {}
        mock_boto3_client.return_value = mock_s3

        result = create_seg_values_for_gl_code(self.mock_df, 123, "2023-08-18", "mock_bucket")

        self.assertEqual(result, {
            "File_date": "2023-08-18",
            "GL_Code": 123,
            "data_path": "processing/gl_codes/123/data.json"
        })

    @patch("boto3.client")
    def test_create_seg_values_for_gl_code_filtering_failure(self, mock_boto3_client):
        with self.assertRaises(Exception) as context:
            create_seg_values_for_gl_code(pd.DataFrame(), 123, "2023-08-18", "mock_bucket")

        self.assertIn("'GL Code'", str(context.exception))

    @patch("boto3.client")
    def test_create_seg_values_for_gl_code_upload_failure(self, mock_boto3_client):
        mock_s3 = Mock()
        mock_s3.put_object.side_effect = Exception("S3 upload failed")
        mock_boto3_client.return_value = mock_s3

        with self.assertRaises(Exception) as context:
            create_seg_values_for_gl_code(self.mock_df, 123, "2023-08-18", "mock_bucket")

        self.assertIn("S3 upload failed", str(context.exception))
        
    @patch("parser.create_df_from_csv_in_s3", return_value=pd.DataFrame())
    @patch("parser.create_seg_values_for_gl_code", return_value={})
    @patch("parser.create_df_for_table_ppt", return_value=pd.DataFrame())
    @patch.dict('os.environ', {'table_ppt_key': 'mock_table_ppt_key'})
    @patch("parser.create_edited_template", return_value=True)
    def test_lambda_handler_success(self, mock_create_edited_template,
                                    mock_create_df_for_table_ppt,
                                    mock_create_seg_values_for_gl_code,
                                    mock_create_df_from_csv_in_s3):
        mock_create_df_from_csv_in_s3.return_value = self.mock_df
        mock_create_seg_values_for_gl_code.return_value = self.mock_output_record
        mock_create_df_for_table_ppt.return_value = self.mock_table_ppt_df
        mock_create_edited_template.return_value = self.mock_template
        
        from parser import lambda_handler
        result = lambda_handler(self.mock_event, {})
        
        expected_output = {
            "Records": [self.mock_output_record, self.mock_output_record, self.mock_output_record]
        }
        expected_response = {
            "Status": "Success",
            "Output": expected_output
        }
        self.assertEqual(result, expected_response)

if __name__ == '__main__':
    unittest.main()