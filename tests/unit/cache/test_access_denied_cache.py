import unittest
from botocore import credentials
from botocore.exceptions import ClientError
from aws_s3_access_grants_boto3_plugin.cache.access_denied_cache import AccessDeniedCache
from aws_s3_access_grants_boto3_plugin.cache.cache_key import CacheKey


class TestAccessDeniedCache(unittest.TestCase):
    access_denied_cache = AccessDeniedCache()

    def test_cache_key_equals(self):
        credentials_1 = credentials.Credentials(access_key="access_key", secret_key="secret_key", token="token")
        permission_1 = 'READ'
        s3_prefix_1 = "s3://bucket-name/*"
        key_1 = CacheKey(credentials_1, permission_1, s3_prefix_1)

        credentials_2 = credentials.Credentials(access_key="access_key", secret_key="secret_key", token="token")
        permission_2 = 'READ'
        s3_prefix_2 = "s3://bucket-name/*"
        key_2 = CacheKey(credentials_2, permission_2, s3_prefix_2)

        error_response = {'Error': {'Message': 'You do not have READWRITE permissions to the requested S3 Prefix: '
                                               's3://bucket-name/*', 'Code': 'AccessDenied'}}
        e = ClientError(error_response, "HeadBucket")

        self.access_denied_cache.put_value_in_cache(key_1, e)
        self.assertIsNotNone(self.access_denied_cache.get_value_from_cache(key_1))
        self.assertIsNotNone(self.access_denied_cache.get_value_from_cache(key_2))