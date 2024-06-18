import json
import logging
import random
import string
import time
import boto3
from botocore.exceptions import ClientError


class SetupIntegrationTests:
    test_account = ""   # Set your account number here
    iam_role_name = 'aws-s3-access-grants-sdk-plugin-integration-role'
    iam_client = boto3.client('iam')
    region = 'us-west-1'
    s3_client = boto3.client('s3', region_name=region)
    s3_control_client = boto3.client('s3control', region_name=region)
    registered_bucket_name = 'boto3-plugin-test-bucket-'+''.join(random.choices(string.ascii_lowercase, k=10))
    unregistered_bucket_name = 'unregistered-boto3-plugin-test-bucket-'+''.join(random.choices(string.ascii_lowercase, k=10))
    content = "s3 AG content"
    TEST_LOCATION_1 = "PrefixA/"
    TEST_LOCATION_2 = "PrefixA/PrefixB/"
    TEST_OBJECT_1 = TEST_LOCATION_1 + "file1.txt"
    TEST_OBJECT_2 = TEST_LOCATION_2 + "file2.txt"
    ALLOWED_BUCKET_PREFIX_1 = TEST_LOCATION_1 + "*"
    ALLOWED_BUCKET_PREFIX_2 = TEST_LOCATION_2 + "*"
    grant_list = []
    access_grants_location_id = None
    iam_policy_arn = None

    def test_setup(self):
        logging.debug("Creating test resources.")
        self.iam_policy_arn = self.create_iam_policy()
        iam_role_arn = self.create_iam_role()
        self.attach_role_policy(self.iam_role_name, self.iam_policy_arn)
        logging.debug("Created IAM resources.")
        self.create_bucket(self.registered_bucket_name, self.region)
        self.create_bucket(self.unregistered_bucket_name, self.region)
        self.put_object(self.registered_bucket_name, self.TEST_OBJECT_1, self.content)
        self.put_object(self.registered_bucket_name, self.TEST_OBJECT_2, self.content)
        self.put_object(self.unregistered_bucket_name, self.TEST_OBJECT_1, self.content)
        logging.debug("Created S3 resources.")
        time.sleep(5)  # IAM role takes time to be available
        self.create_access_grants_instance(self.test_account)
        access_grants_location = 's3://' + self.registered_bucket_name
        self.access_grants_location_id = self.create_access_grants_location(access_grants_location, self.test_account,
                                                                            iam_role_arn)
        self.grant_list.append(self.create_access_grant(self.test_account, self.access_grants_location_id, 'READ',
                                                        self.ALLOWED_BUCKET_PREFIX_1,
                                                        iam_role_arn, access_grants_location))

        self.grant_list.append(self.create_access_grant(self.test_account, self.access_grants_location_id, 'READWRITE',
                                                        self.ALLOWED_BUCKET_PREFIX_2,
                                                        iam_role_arn, access_grants_location))
        logging.debug("Created S3 Access Grants resources")

    def create_iam_policy(self):
        policy_name = 'iam-integ-test-policy-for-boto3-plugin'
        policy_doc = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:PutObject",
                        "s3:GetObject",
                        "s3:DeleteObject"
                    ],
                    "Resource": [
                        "arn:aws:s3:::access-grants-boto3-test-bucket/*"
                    ]
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:ListBucket"
                    ],
                    "Resource": [
                        "arn:aws:s3:::access-grants-boto3-test-bucket"
                    ]
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "sts:*"
                    ],
                    "Resource": [
                        "arn:aws:s3:::access-grants-boto3-test-bucket"
                    ]
                }
            ]
        }

        try:
            response = self.iam_client.create_policy(
                PolicyName=policy_name,
                PolicyDocument=json.dumps(policy_doc)
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                return "arn:aws:iam::" + self.test_account + ":policy/" + policy_name
            else:
                raise e
        else:
            return response['Policy']['Arn']

    def create_iam_role(self):
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "Stmt1685556427189",
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "access-grants.s3.amazonaws.com"
                    },
                    "Action": [
                        "sts:AssumeRole"
                    ]
                }
            ]
        }
        try:
            response = self.iam_client.create_role(
                RoleName=self.iam_role_name, AssumeRolePolicyDocument=json.dumps(trust_policy)
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                return "arn:aws:iam::" + self.test_account + ":role/" + self.iam_role_name
            else:
                raise e
        else:
            return response['Role']['Arn']

    def attach_role_policy(self, role_name, policy_arn):
        self.iam_client.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)

    def create_bucket(self, bucket_name, region):
        try:
            self.s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={
                'LocationConstraint': region})
        except ClientError as e:
            if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                pass
            else:
                raise e

    def put_object(self, bucket_name, object_name, content):
        self.s3_client.put_object(Bucket=bucket_name,
                                  Key=object_name,
                                  Body=content)

    def create_access_grants_instance(self, account_id):
        try:
            response = self.s3_control_client.create_access_grants_instance(AccountId=account_id)
            return response['AccessGrantsInstanceArn']
        except ClientError as e:
            if e.response['Error']['Code'] == 'AccessGrantsInstanceAlreadyExists':
                response = self.s3_control_client.get_access_grants_instance(AccountId=account_id)
                return response['AccessGrantsInstanceArn']
            else:
                raise e

    def create_access_grants_location(self, s3_prefix, account_id, iam_role_arn):
        try:
            response = self.s3_control_client.create_access_grants_location(AccountId=account_id,
                                                                            LocationScope=s3_prefix,
                                                                            IAMRoleArn=iam_role_arn)
            return response['AccessGrantsLocationId']
        except ClientError as e:
            if e.response['Error']['Code'] == 'AccessGrantsLocationAlreadyExistsError':
                response = self.s3_control_client.list_access_grants_locations(AccountId=account_id,
                                                                               LocationScope=s3_prefix)
                for location in response['AccessGrantsLocationsList']:
                    if location['IAMRoleArn'] == iam_role_arn:
                        return location['AccessGrantsLocationId']
            else:
                raise e

    def create_access_grant(self, account_id, location_id, permission, s3_prefix, grantee, access_grants_location):
        try:
            response = self.s3_control_client.create_access_grant(AccountId=account_id,
                                                                  AccessGrantsLocationId=location_id,
                                                                  AccessGrantsLocationConfiguration={
                                                                      'S3SubPrefix': s3_prefix
                                                                  },
                                                                  Grantee={
                                                                      'GranteeType': 'IAM',
                                                                      'GranteeIdentifier': grantee
                                                                  },
                                                                  Permission=permission)
            return response['AccessGrantId']
        except ClientError as e:
            if e.response['Error']['Code'] == 'AccessGrantAlreadyExists':
                grant_scope = access_grants_location + '/' + s3_prefix
                response = self.s3_control_client.list_access_grants(AccountId=account_id,
                                                                     GranteeType='IAM',
                                                                     GranteeIdentifier=grantee,
                                                                     Permission=permission,
                                                                     GrantScope=grant_scope)
                return response['AccessGrantsList'][0]['AccessGrantId']
            else:
                raise e

    def teardown(self):
        logging.debug('Deleting test resources')
        self.delete_access_grants()
        self.delete_access_grants_location()
        self.delete_access_grants_instance()
        logging.debug("Deleted S3 Access Grants resources.")
        self.delete_object(self.registered_bucket_name, self.TEST_OBJECT_1)
        self.delete_object(self.registered_bucket_name, self.TEST_OBJECT_2)
        self.delete_object(self.unregistered_bucket_name, self.TEST_OBJECT_1)
        self.delete_bucket(self.registered_bucket_name)
        self.delete_bucket(self.unregistered_bucket_name)
        logging.debug("Deleted S3 resources.")
        self.detach_role_policy(self.iam_role_name, self.iam_policy_arn)
        self.delete_iam_policy(self.iam_policy_arn)
        logging.debug("Deleted IAM resources.")

    def delete_access_grants(self):
        try:
            for grant in self.grant_list:
                self.s3_control_client.delete_access_grant(AccountId=self.test_account, AccessGrantId=grant)
        except ClientError as e:
            logging.debug("Error while deleting access grants." + e.response['Error']['Message'])

    def delete_access_grants_location(self):
        try:
            self.s3_control_client.delete_access_grants_location(AccountId=self.test_account,
                                                                 AccessGrantsLocationId=self.access_grants_location_id)
        except ClientError as e:
            logging.debug("Error while deleting access grants location." + e.response['Error']['Message'])

    def delete_access_grants_instance(self):
        try:
            self.s3_control_client.delete_access_grants_instance(AccountId=self.test_account)
        except ClientError as e:
            logging.debug("Error while deleting access grants instance." + e.response['Error']['Message'])

    def delete_object(self, bucket, key):
        try:
            self.s3_client.delete_object(Bucket=bucket, Key=key)
        except ClientError as e:
            logging.debug("Error while deleting S3 object." + e.response['Error']['Message'])

    def delete_bucket(self, bucket):
        try:
            self.s3_client.delete_bucket(Bucket=bucket)
        except ClientError as e:
            logging.debug("Error while deleting S3 bucket." + e.response['Error']['Message'])

    def detach_role_policy(self, role_name, policy_arn):
        try:
            self.iam_client.detach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
        except ClientError as e:
            logging.debug("Error while detaching role policy." + e.response['Error']['Message'])

    def delete_iam_policy(self, policy_arn):
        try:
            self.iam_client.delete_policy(PolicyArn=policy_arn)
        except ClientError as e:
            logging.debug("Error while deleting IAM policy." + e.response['Error']['Message'])

