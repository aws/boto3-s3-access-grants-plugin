import unittest
from botocore import credentials
from aws_s3_access_grants_boto3_plugin.cache.cache_key import CacheKey


class TestCacheKey(unittest.TestCase):
    # verify if both the keys are equal if they are equal in value not object id
    def test_cache_key_equals(self):
        credentials_1 = credentials.Credentials(access_key="access_key", secret_key="secret_key", token="token")
        credentials_2 = credentials.Credentials(access_key="access_key", secret_key="secret_key", token="token")
        permission_1 = 'READ'
        permission_2 = 'READ'
        s3_prefix_1 = "s3://bucket-name/*"
        s3_prefix_2 = "s3://bucket-name/*"
        key_1 = CacheKey(credentials_1, permission_1, s3_prefix_1)
        key_2 = CacheKey(credentials_2, permission_2, s3_prefix_2)
        self.assertEqual(key_1, key_2)

    # verify equals fails if the value is different for both the objects
    def test_cache_key_not_equals(self):
        credentials_1 = credentials.Credentials(access_key="access_key_1", secret_key="secret_key_1", token="token_1")
        credentials_2 = credentials.Credentials(access_key="access_key_2", secret_key="secret_key_2", token="token_2")
        permission_1 = 'READ'
        permission_2 = 'WRITE'
        s3_prefix_1 = "s3://bucket-name/*"
        s3_prefix_2 = "s3://bucket-name/*"
        key_1 = CacheKey(credentials_1, permission_1, s3_prefix_1)
        key_2 = CacheKey(credentials_2, permission_2, s3_prefix_2)
        self.assertNotEqual(key_1, key_2)

    def test_cache_key_init_method(self):
        requester_credentials = credentials.Credentials(access_key="access_key", secret_key="secret_key", token="token")
        cache_key = CacheKey(requester_credentials, 'READ', "s3://bucket-name/*")
        cache_key_1 = CacheKey(cache_key=cache_key, permission='WRITE')
        cache_key_2 = CacheKey(cache_key=cache_key, s3_prefix="s3://*")
        self.assertEqual(cache_key_1.credentials, cache_key.credentials)
        self.assertEqual(cache_key_1.s3_prefix, cache_key.s3_prefix)
        self.assertEqual(cache_key_1.permission, 'WRITE')
        self.assertEqual(cache_key_2.credentials, cache_key.credentials)
        self.assertEqual(cache_key_2.permission, cache_key.permission)
        self.assertEqual(cache_key_2.s3_prefix, "s3://*")