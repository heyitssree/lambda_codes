import aws_cdk.aws_iam as iam


class IAMConstruct:
    
    
    @staticmethod
    def get_kms_policy_document(
        env: str,
        config: dict
    ) -> iam.PolicyDocument:
        """Creates KMS Policy Document."""
        policy_docuement = iam.PolicyDocument()
        policy_statement = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "kms:Decrypt",
                "kms:Encrypt",
                "kms:Create*",
                "kms:Describe*",
                "kms:Enable*",
                "kms:List*",
                "kms:Put*",
                "kms:Update*",
                "kms:Revoke*",
                "kms:Disable*",
                "kms:Get",
                "kms:Delete*",
                "kms:ScheduleKeyDeletion",
                "kms:CancelKeyDeletion"               
            ],
            resources=["*"]
        )
        policy_docuement.add_statements(policy_statement)
        policy_docuement.add_account_root_principal()
        return policy_docuement