import unittest
from datetime import datetime
from unittest.mock import patch
import mock
from botocore import credentials
from aws_s3_access_grants_boto3_plugin.cache.cache_key import CacheKey
from aws_s3_access_grants_boto3_plugin.exceptions import UnsupportedOperationError
from aws_s3_access_grants_boto3_plugin.s3_access_grants_plugin import S3AccessGrantsPlugin


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

    def test_should_fallback_to_default_credentials_when_fallback_is_enabled(self):
        s3_client = mock.Mock()
        plugin = S3AccessGrantsPlugin(s3_client, True)
        e = mock.Mock()
        self.assertTrue(plugin._S3AccessGrantsPlugin__should_fallback_to_default_credentials_for_this_case(e))

    @patch('aws_s3_access_grants_boto3_plugin.s3_access_grants_plugin.AccessGrantsCache.get_credentials')
    def test_get_value_from_cache(self, get_credentials_mock):
        s3_client = mock.Mock()
        s3_control_client = mock.Mock()
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
        self.assertEqual(plugin._S3AccessGrantsPlugin__get_value_from_cache(cache_key, s3_control_client, '123456789012'), access_grants_credentials)

    def test_get_common_prefix_for_multiple_prefixes(self):
        s3_client = mock.Mock()
        plugin = S3AccessGrantsPlugin(s3_client, False)
        prefix_list = ["folder/path123/A/logs","folder/path234/A/logs","folder/path234/A/artifacts"]
        self.assertEqual(plugin._S3AccessGrantsPlugin__get_common_prefix_for_multiple_prefixes(prefix_list), '/folder/path')
        prefix_list = ["ABC/A/B/C/log.txt", "ABC/B/A/C/log.txt", "ABC/C/A/B/log.txt"]
        self.assertEqual(plugin._S3AccessGrantsPlugin__get_common_prefix_for_multiple_prefixes(prefix_list),
                         '/ABC/')
        prefix_list = ["A/B/C/log.txt", "B/A/C/log.txt", "C/A/B/log.txt"]
        self.assertEqual(plugin._S3AccessGrantsPlugin__get_common_prefix_for_multiple_prefixes(prefix_list),
                         '/')
        prefix_list = ["ABC/A/B/C/log.txt", "ABC/B/A/C/log.txt", "ABC/C/A/B/log.txt", "XYZ/X/Y/Y/log.txt", "XYZ/Y/X/Z/log.txt", "XYZ/Z/X/Y/log.txt"]
        self.assertEqual(plugin._S3AccessGrantsPlugin__get_common_prefix_for_multiple_prefixes(prefix_list),'/')
        prefix_list = []
        self.assertEqual(plugin._S3AccessGrantsPlugin__get_common_prefix_for_multiple_prefixes(prefix_list), '/')
        prefix_list = ["ABC/A/B/C/log.txt"]
        self.assertEqual(plugin._S3AccessGrantsPlugin__get_common_prefix_for_multiple_prefixes(prefix_list), '/ABC/A/B/C/log.txt')
        prefix_list = ["ABC/A/B/C/log.txt","ABC/A/B/C/log.txt"]
        self.assertEqual(plugin._S3AccessGrantsPlugin__get_common_prefix_for_multiple_prefixes(prefix_list), '/ABC/A/B/C/log.txt')

    @patch('aws_s3_access_grants_boto3_plugin.s3_access_grants_plugin.BucketRegionResolverCache.resolve')
    def test_get_s3_control_client_for_region(self, mock_resolve):
        s3_client = mock.Mock()
        plugin = S3AccessGrantsPlugin(s3_client, False)
        mock_resolve.return_value = 'us-east-1'
        plugin._S3AccessGrantsPlugin__get_s3_control_client_for_region("bucket-name")
        self.assertTrue(plugin.client_dict.__contains__('us-east-1'))


