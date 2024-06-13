import time
import botocore.session
from botocore.exceptions import ClientError
from s3_access_grants_plugin import S3AccessGrantsPlugin
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

    def test_plugin_grant_present(self):
        self.createS3Client(enable_fallback=False)
        response = self.s3_client.get_object(Bucket=self.test_setup.registered_bucket_name, Key=self.test_setup.TEST_OBJECT_1)
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)

    def test_plugin_grant_absent_fallback_disabled(self):
        self.createS3Client(enable_fallback=False)
        try:
            self.s3_client.get_object(Bucket=self.test_setup.unregistered_bucket_name,
                                                 Key=self.test_setup.TEST_OBJECT_1)
        except ClientError as e:
            self.assertEqual(e.response['ResponseMetadata']['HTTPStatusCode'], 403)

    def test_plugin_grant_absent_fallback_enabled(self):
        self.createS3Client(enable_fallback=True)
        response = self.s3_client.get_object(Bucket=self.test_setup.unregistered_bucket_name, Key=self.test_setup.TEST_OBJECT_1)
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)