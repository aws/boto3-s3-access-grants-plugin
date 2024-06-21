import botocore.session
from botocore.exceptions import ClientError
from aws_s3_access_grants_boto3_plugin.s3_access_grants_plugin import S3AccessGrantsPlugin
from tests.integration.test_setup import SetupIntegrationTests
import unittest


class TestPlugin(unittest.TestCase):
    test_setup = SetupIntegrationTests()
    session = botocore.session.get_session()
    s3_client = None
    plugin = None
    access_key_id = None
    secret_access_key = None
    session_token = None
    sts_client = None

    @classmethod
    def setUpClass(cls):
        cls.test_setup.test_setup()

    @classmethod
    def tearDownClass(cls):
        cls.test_setup.teardown()

    def createS3Client(self, enable_fallback):
        self.s3_client = self.session.create_client('s3')
        self.plugin = S3AccessGrantsPlugin(self.s3_client, enable_fallback)
        self.plugin.register()

    def createS3ClientInSameRegion(self, enable_fallback):
        self.s3_client = self.session.create_client('s3', region_name='us-west-1')
        self.plugin = S3AccessGrantsPlugin(self.s3_client, enable_fallback)
        self.plugin.register()

    def test_grant_present(self):
        self.createS3Client(enable_fallback=False)
        response = self.s3_client.get_object(Bucket=self.test_setup.registered_bucket_name, Key=self.test_setup.TEST_OBJECT_1)
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)

    def test_grant_absent_fallback_disabled(self):
        self.createS3Client(enable_fallback=False)
        try:
            self.s3_client.get_object(Bucket=self.test_setup.unregistered_bucket_name,
                                                 Key=self.test_setup.TEST_OBJECT_1)
        except ClientError as e:
            self.assertEqual(e.response['ResponseMetadata']['HTTPStatusCode'], 403)

    def test_grant_absent_fallback_enabled(self):
        self.createS3Client(enable_fallback=True)
        response = self.s3_client.get_object(Bucket=self.test_setup.unregistered_bucket_name, Key=self.test_setup.TEST_OBJECT_1)
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)

    def test_unsupported_operation_fallback_disabled(self):
        self.createS3Client(enable_fallback=False)
        response = self.s3_client.list_buckets()
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)

    def test_grant_present_same_region(self):
        self.createS3ClientInSameRegion(enable_fallback=False)
        response = self.s3_client.get_object(Bucket=self.test_setup.registered_bucket_name, Key=self.test_setup.TEST_OBJECT_2)
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)

    def test_WRITE_grant_present(self):
        self.createS3Client(enable_fallback=False)
        response = self.s3_client.put_object(Bucket=self.test_setup.registered_bucket_name,
                                  Key=self.test_setup.TEST_LOCATION_2 + 'file.txt',
                                  Body=self.test_setup.content)
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
        response = self.s3_client.delete_object(Bucket=self.test_setup.registered_bucket_name,
                                                Key=self.test_setup.TEST_LOCATION_2 + 'file.txt')
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 204)

    def test_WRITE_grant_absent(self):
        self.createS3Client(enable_fallback=False)
        try:
            self.s3_client.put_object(Bucket=self.test_setup.registered_bucket_name,
                                      Key=self.test_setup.TEST_LOCATION_1 + 'file.txt',
                                      Body=self.test_setup.content)
        except ClientError as e:
            self.assertEqual(e.response['ResponseMetadata']['HTTPStatusCode'], 403)

    def test_copy_object_grant_present(self):
        self.createS3Client(enable_fallback=False)
        response = self.s3_client.copy_object(Bucket=self.test_setup.registered_bucket_name, Key=self.test_setup.TEST_LOCATION_2 + "copiedFile.txt",
                                         CopySource={'Bucket': self.test_setup.registered_bucket_name, 'Key': self.test_setup.TEST_OBJECT_2})
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
        response = self.s3_client.delete_object(Bucket=self.test_setup.registered_bucket_name,
                                                Key=self.test_setup.TEST_LOCATION_2 + 'copiedFile.txt')
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 204)

    def test_delete_objects_grant_present(self):
        self.createS3Client(enable_fallback=False)
        self.s3_client.put_object(Bucket=self.test_setup.registered_bucket_name,
                                  Key=self.test_setup.TEST_LOCATION_2 + 'test_file_1.txt',
                                  Body=self.test_setup.content)
        self.s3_client.put_object(Bucket=self.test_setup.registered_bucket_name,
                                  Key=self.test_setup.TEST_LOCATION_2 + 'test_file_2.txt',
                                  Body=self.test_setup.content)
        self.s3_client.put_object(Bucket=self.test_setup.registered_bucket_name,
                                  Key=self.test_setup.TEST_LOCATION_2 + 'test_file_3.txt',
                                  Body=self.test_setup.content)
        response = self.s3_client.delete_objects(Bucket=self.test_setup.registered_bucket_name, Delete={
            'Objects': [
                {'Key': self.test_setup.TEST_LOCATION_2 + 'test_file_1.txt'},
                {'Key': self.test_setup.TEST_LOCATION_2 + 'test_file_2.txt'},
                {'Key': self.test_setup.TEST_LOCATION_2 + 'test_file_3.txt'}
            ]
        })
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
