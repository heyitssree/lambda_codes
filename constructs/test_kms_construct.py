from unittest.mock import patch, Mock
from aws_cdk.aws_iam import PolicyDocument
from aws_cdk.aws_kms import Key
from constructs.kms_construct import KMSConstruct
import unittest

class TestKMSConstruct(unittest.TestCase):
    """KMS Construct testing class."""
    
    def setUp(self):
        self.stack = Mock()
        self.env = "test"
        self.config = {'test': {'appName': 'test-app-name'}}
        self.policy_doc = Mock(spec=PolicyDocument)
        
    @patch('aws_cdk.aws_kms.Key')
    def test_create_kms_key(self, MockKey):
        # Arrange
        key_instance = Mock(spec=Key)
        MockKey.return_value = key_instance

        expected_params = {
            'scope': self.stack,
            'id': f"{self.config[self.env]['appName']}-key-Id",
            'alias': f"{self.config[self.env]['appName']}-kms",
            'enabled': True,
            'enable_key_rotation': True,
            'policy': self.policy_doc
        }
        # Act
        result = KMSConstruct.create_kms_key(self.stack, self.env, self.config, self.policy_doc)
        
        # Assert
        MockKey.assert_called_once_with(**expected_params)
        self.assertEqual(result, expected_params)
        
if __name__ == '__main__':
    unittest.main()