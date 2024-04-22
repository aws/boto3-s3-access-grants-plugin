import unittest

from botocore import credentials

from cache_key import CacheKey


class TestCacheKey(unittest.TestCase):
    # verify if both the keys are equal if they are equal in value not object id
    def test_cache_key_equals(self):
        credentials1 = credentials.Credentials(access_key="access_key", secret_key="secret_key", token="token")
        credentials2 = credentials.Credentials(access_key="access_key", secret_key="secret_key", token="token")
        permission1 = 'READ'
        permission2 = 'READ'
        s3_prefix1 = "s3://s3-staircase-integration-sdkv2-codeshare/*"
        s3_prefix2 = "s3://s3-staircase-integration-sdkv2-codeshare/*"
        key1 = CacheKey(credentials1, permission1, s3_prefix1)
        key2 = CacheKey(credentials2, permission2, s3_prefix2)
        self.assertEqual(key1, key2)

    # verify equals fails if the value is different for both the objects
    def test_cache_key_not_equals(self):
        credentials1 = credentials.Credentials(access_key="access_key_1", secret_key="secret_key_1", token="token_1")
        credentials2 = credentials.Credentials(access_key="access_key_2", secret_key="secret_key_2", token="token_2")
        permission1 = 'READ'
        permission2 = 'WRITE'
        s3_prefix1 = "s3://s3-staircase-integration-sdkv2-codeshare/*"
        s3_prefix2 = "s3://s3-staircase-integration-sdkv2-codeshare/*"
        key1 = CacheKey(credentials1, permission1, s3_prefix1)
        key2 = CacheKey(credentials2, permission2, s3_prefix2)
        self.assertNotEqual(key1, key2)