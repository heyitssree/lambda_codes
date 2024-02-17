import unittest
from unittest.mock import patch, Mock, call
from url_generator import lambda_handler

class TestUrlGenerator(unittest.TestCase):

    @patch.dict('os.environ', {'bucket_name': 'test-bucket'})
    @patch('url_generator.boto3.client')
    def test_lambda_handler_valid_event(self, mock_boto_client):
        mock_s3 = Mock()
        mock_s3.list_objects_v2.return_value = {'Contents': ['some_content']}
        mock_boto_client.return_value = mock_s3

        event = {"valDate": "2023-08-18"}

        response = lambda_handler(event, {})
        self.assertEqual(response["statusCode"], 200)
        self.assertIn("Url", response)

    @patch.dict('os.environ', {'bucket_name': 'test-bucket'})
    @patch('url_generator.boto3.client')
    def test_lambda_handler_file_not_in_s3(self, mock_boto_client):
        mock_s3 = Mock()
        mock_s3.list_objects_v2.return_value = {}  # No 'Contents'
        mock_boto_client.return_value = mock_s3

        event = {"valDate": "2023-08-18"}

        with self.assertRaises(OSError) as context:
            lambda_handler(event, {})

        self.assertIn("Couldn't find file", str(context.exception))

if __name__ == '__main__':
    unittest.main()
