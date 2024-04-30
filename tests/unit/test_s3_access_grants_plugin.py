import unittest
from datetime import datetime
from unittest.mock import patch
import mock
from botocore import credentials
from cache_key import CacheKey
from exceptions import UnsupportedOperationError
from s3_access_grants_plugin import S3AccessGrantsPlugin


class TestS3AccessGrantsPlugin(unittest.TestCase):

    def test_should_fallback_to_default_credentials_for_unsupported_operations(self):
        s3_client = mock.Mock()
        plugin = S3AccessGrantsPlugin(s3_client, False)
        e = UnsupportedOperationError("Access Grants does not support the requested operation.")
        self.assertTrue(plugin._S3AccessGrantsPlugin__should_fallback_to_default_credentials_for_this_case(e))

    def test_should_not_fallback_to_default_credentials_for_exceptions(self):
        s3_client = mock.Mock()
        plugin = S3AccessGrantsPlugin(s3_client, False)
        e = mock.Mock()
        self.assertFalse(plugin._S3AccessGrantsPlugin__should_fallback_to_default_credentials_for_this_case(e))

    @patch('s3_access_grants_plugin.AccessGrantsCache.get_credentials')
    def test_get_value_from_cache(self, get_credentials_mock):
        s3_client = mock.Mock()
        requester_credentials = credentials.Credentials(access_key="access_key", secret_key="secret_key", token="token")
        cache_key = CacheKey(requester_credentials, 'READ', "s3://bucket/name")
        access_grants_credentials = {
            'Credentials': {
                'AccessKeyId': 'access_key_id',
                'SecretAccessKey': 'secret_access_key',
                'SessionToken': 'session_token',
                'Expiration': datetime(2015, 1, 1)
            }
        }
        get_credentials_mock.return_value = access_grants_credentials
        plugin = S3AccessGrantsPlugin(s3_client, False)
        self.assertEqual(plugin._S3AccessGrantsPlugin__get_value_from_cache(cache_key, '123456789012'), access_grants_credentials)
