
import unittest
from unittest.mock import patch, Mock
import json

# Assuming the refactored_script is saved as `trigger.py`
# from trigger import lambda_handler, ...

# For the sake of this execution, we're defining mock functions here
def lambda_handler(event, context):
    pass

class TestTriggerScript(unittest.TestCase):

    def setUp(self):
        self.sample_event = {
            'year': '2023',
            'month': '08',
            'day': '15'
        }
        self.context = {}

    def test_lambda_handler(self):
        # Mocking returned values
        mock_get_env.side_effect = ['query_key_value', 'bucket_name_value', 'query_bucket_name_value']
        mock_fetch_query.return_value = 'SELECT * FROM TABLE;'
        mock_execute_athena.return_value = {'QueryExecutionId': 'sample_id'}
        mock_check_query_status.return_value = 'SUCCEEDED'
        mock_start_step_function.return_value = {}

        # Executing the lambda handler
        lambda_handler(self.sample_event, self.context)

        # Assertions to ensure the functions were called
        self.assertEqual(mock_get_env.call_count, 3)
        self.assertEqual(mock_fetch_query.call_count, 3)
        self.assertEqual(mock_execute_athena.call_count, 3)
        self.assertEqual(mock_check_query_status.call_count, 3)
        self.assertEqual(mock_copy_in_s3.call_count, 3)
        self.assertEqual(mock_start_step_function.call_count, 1)

# Additional tests for other functions can be added similarly.

if __name__ == '__main__':
    unittest.main()
