import aws_cdk.aws_iam as iam
import aws_cdk.aws_kms as kms
from aws_cdk import Stack


class KMSConstruct:
    """Class for methods to create KMS Key."""
    
    @staticmethod
    def create_kms_key(
        stack: Stack,
        env: str,
        config: dict,
        policy_doc: iam.PolicyDocument
    ) -> kms.Key:
        """Creates KMS Key."""
        return kms.Key(
            scope=stack,
            id=f"{config[env]['appName']}-key-Id",
            alias=f"{config[env]['appName']}-kms",
            description=f"{config[env]['appName']}-kms-key",
            enabled=True,
            enable_key_rotation=True,
            policy=policy_doc
        )
        