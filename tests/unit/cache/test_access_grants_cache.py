import time
import unittest
from datetime import datetime
from botocore import credentials
from aws_s3_access_grants_boto3_plugin.exceptions import IllegalArgumentException
from aws_s3_access_grants_boto3_plugin.cache.access_denied_cache import AccessDeniedCache
from aws_s3_access_grants_boto3_plugin.cache.access_grants_cache import AccessGrantsCache
from aws_s3_access_grants_boto3_plugin.cache.cache_key import CacheKey
import mock


class TestAccessGrantsCache(unittest.TestCase):
    access_grants_cache = None
    access_grants_credentials = credentials.Credentials(access_key="access_key", secret_key="secret_key", token="token")
    mock_s3_control_client = None
    mock_account_id_resolver_cache = None
    access_denied_cache = None
    requester_account_id = "123456789012"

    def setUp(self):
        self.access_grants_cache = AccessGrantsCache()
        self.mock_s3_control_client = mock.Mock()
        self.mock_account_id_resolver_cache = mock.Mock()
        self.access_denied_cache = AccessDeniedCache()

    def test_cache_hit(self):
        credentials_1 = credentials.Credentials(access_key="access_key", secret_key="secret_key", token="token")
        key_1 = CacheKey(credentials_1, 'READ', "s3://bucket-name")
        credentials_2 = credentials.Credentials(access_key="access_key", secret_key="secret_key", token="token")
        key_2 = CacheKey(credentials_2, 'READ', "s3://bucket-name")

        self.access_grants_cache._AccessGrantsCache__put_value_in_cache(key_1, self.access_grants_credentials)
        value_1 = self.access_grants_cache.get_credentials(self.mock_s3_control_client, key_1,
                                                           self.requester_account_id, self.access_denied_cache)
        value_2 = self.access_grants_cache.get_credentials(self.mock_s3_control_client, key_2,
                                                           self.requester_account_id, self.access_denied_cache)
        self.assertEqual(value_1, value_2)

    def test_cache_hit_for_grant_present_at_higher_prefix(self):
        credentials_1 = credentials.Credentials(access_key="access_key", secret_key="secret_key", token="token")
        key_1 = CacheKey(credentials_1, 'READ', "s3://bucket-name")
        self.access_grants_cache._AccessGrantsCache__put_value_in_cache(key_1, self.access_grants_credentials)

        credentials_2 = credentials.Credentials(access_key="access_key", secret_key="secret_key", token="token")
        key_2 = CacheKey(credentials_2, 'READ', "s3://bucket-name/prefixA")

        value = self.access_grants_cache.get_credentials(self.mock_s3_control_client, key_2,
                                                         self.requester_account_id, self.access_denied_cache)
        self.assertEqual(value, self.access_grants_credentials)

    def test_cache_hit_for_grant_present_at_higher_prefix_at_character_level(self):
        credentials_1 = credentials.Credentials(access_key="access_key", secret_key="secret_key", token="token")
        key_1 = CacheKey(credentials_1, 'READ', "s3://bucket-name/P*")
        self.access_grants_cache._AccessGrantsCache__put_value_in_cache(key_1, self.access_grants_credentials)

        credentials_2 = credentials.Credentials(access_key="access_key", secret_key="secret_key", token="token")
        key_2 = CacheKey(credentials_2, 'READ', "s3://bucket-name/PrefixA")

        value = self.access_grants_cache.get_credentials(self.mock_s3_control_client, key_2,
                                                         self.requester_account_id, self.access_denied_cache)
        self.assertEqual(value, self.access_grants_credentials)

    def test_cache_hit_for_readwrite_grant_for_a_read_request(self):
        credentials_1 = credentials.Credentials(access_key="access_key", secret_key="secret_key", token="token")
        key_1 = CacheKey(credentials_1, 'READWRITE', "s3://bucket-name")
        self.access_grants_cache._AccessGrantsCache__put_value_in_cache(key_1, self.access_grants_credentials)

        credentials_2 = credentials.Credentials(access_key="access_key", secret_key="secret_key", token="token")
        key_2 = CacheKey(credentials_2, 'READ', "s3://bucket-name/prefixA")

        value = self.access_grants_cache.get_credentials(self.mock_s3_control_client, key_2,
                                                         self.requester_account_id, self.access_denied_cache)
        self.assertEqual(value, self.access_grants_credentials)

    @mock.patch('aws_s3_access_grants_boto3_plugin.cache.access_grants_cache.AccessGrantsCache._AccessGrantsCache__get_credentials_from_service')
    def test_cache_miss_for_write_grant_for_a_read_request(self, mocked_get_credentials_from_service):
        credentials_1 = credentials.Credentials(access_key="access_key", secret_key="secret_key", token="token")
        key_1 = CacheKey(credentials_1, 'WRITE', "s3://bucket-name")
        self.access_grants_cache._AccessGrantsCache__put_value_in_cache(key_1, self.access_grants_credentials)

        credentials_2 = credentials.Credentials(access_key="access_key", secret_key="secret_key", token="token")
        key_2 = CacheKey(credentials_2, 'READ', "s3://bucket-name/prefixA")

        self.access_grants_cache.get_credentials(self.mock_s3_control_client, key_2,
                                                 self.requester_account_id, self.access_denied_cache)
        mocked_get_credentials_from_service.assert_called_once()

    @mock.patch('aws_s3_access_grants_boto3_plugin.cache.access_grants_cache.AccessGrantsCache._AccessGrantsCache__get_credentials_from_service')
    def test_cache_miss_for_cache_key_with_different_prefix(self, mocked_get_credentials_from_service):
        credentials_1 = credentials.Credentials(access_key="access_key", secret_key="secret_key", token="token")
        key_1 = CacheKey(credentials_1, 'READ', "s3://bucket-name/prefixA")
        self.access_grants_cache._AccessGrantsCache__put_value_in_cache(key_1, self.access_grants_credentials)

        credentials_2 = credentials.Credentials(access_key="access_key", secret_key="secret_key", token="token")
        key_2 = CacheKey(credentials_2, 'READ', "s3://bucket-name/prefixB")

        self.access_grants_cache.get_credentials(self.mock_s3_control_client, key_2,
                                                 self.requester_account_id, self.access_denied_cache)

        mocked_get_credentials_from_service.assert_called_once()

    def test_get_credentials_when_cache_is_empty(self):
        self.mock_s3_control_client.get_data_access.return_value = {
            'Credentials': {
                'AccessKeyId': 'access_key_id',
                'SecretAccessKey': 'secret_access_key',
                'SessionToken': 'session_token',
                'Expiration': datetime(2015, 1, 1)
            },
            'MatchedGrantTarget': 'string'
        }

        self.mock_s3_control_client.get_access_grants_instance_for_prefix.return_value = {
            'AccessGrantsInstanceArn': 'arn:aws:s3:us-east-2:987654321098:access-grants/default',
            'AccessGrantsInstanceId': 'abcdefghijklmnopqrstuvwxyz'
        }
        requester_credentials = credentials.Credentials(access_key="access_key", secret_key="secret_key", token="token")
        key = CacheKey(requester_credentials, 'READ', "s3://bucket-name/prefixA")
        value = self.access_grants_cache.get_credentials(self.mock_s3_control_client, key,
                                                         self.requester_account_id, self.access_denied_cache)

        access_grants_credentials = {
            'AccessKeyId': 'access_key_id',
            'SecretAccessKey': 'secret_access_key',
            'SessionToken': 'session_token',
            'Expiration': datetime(2015, 1, 1)
        }
        self.assertEqual(access_grants_credentials, value)

    def test_cache_hit_for_different_prefixes_with_same_matched_grant_target(self):
        self.mock_s3_control_client.get_data_access.return_value = {
            'Credentials': {
                'AccessKeyId': 'access_key_id',
                'SecretAccessKey': 'secret_access_key',
                'SessionToken': 'session_token',
                'Expiration': datetime(2015, 1, 1)
            },
            'MatchedGrantTarget': 's3://bucket-name/*'
        }

        self.mock_s3_control_client.get_access_grants_instance_for_prefix.return_value = {
            'AccessGrantsInstanceArn': 'arn:aws:s3:us-east-2:987654321098:access-grants/default',
            'AccessGrantsInstanceId': 'abcdefghijklmnopqrstuvwxyz'
        }
        requester_credentials = credentials.Credentials(access_key="access_key", secret_key="secret_key", token="token")
        key_1 = CacheKey(requester_credentials, 'READ', "s3://bucket-name/prefixA")
        key_2 = CacheKey(requester_credentials, 'READ', "s3://bucket-name/prefixB")
        self.access_grants_cache.get_credentials(self.mock_s3_control_client, key_1,
                                                 self.requester_account_id, self.access_denied_cache)
        self.access_grants_cache.get_credentials(self.mock_s3_control_client, key_2,
                                                 self.requester_account_id, self.access_denied_cache)
        self.assertEqual(self.mock_s3_control_client.get_data_access.call_count, 1)

    def test_object_level_grant_is_not_stored_in_cache(self):
        self.mock_s3_control_client.get_data_access.return_value = {
            'Credentials': {
                'AccessKeyId': 'access_key_id',
                'SecretAccessKey': 'secret_access_key',
                'SessionToken': 'session_token',
                'Expiration': datetime(2015, 1, 1)
            },
            'MatchedGrantTarget': 's3://bucket-name/prefixA'
        }

        self.mock_s3_control_client.get_access_grants_instance_for_prefix.return_value = {
            'AccessGrantsInstanceArn': 'arn:aws:s3:us-east-2:987654321098:access-grants/default',
            'AccessGrantsInstanceId': 'abcdefghijklmnopqrstuvwxyz'
        }
        requester_credentials = credentials.Credentials(access_key="access_key", secret_key="secret_key", token="token")
        key = CacheKey(requester_credentials, 'READ', "s3://bucket-name/prefixA")
        self.access_grants_cache.get_credentials(self.mock_s3_control_client, key,
                                                 self.requester_account_id, self.access_denied_cache)
        self.access_grants_cache.get_credentials(self.mock_s3_control_client, key,
                                                 self.requester_account_id, self.access_denied_cache)
        self.assertEqual(self.mock_s3_control_client.get_data_access.call_count, 2)

    def test_which_s3_prefixes_stored_in_cache(self):
        grant_1 = "s3://bucket/foo/bar/*"
        grant_2 = "s3://bucket/foo/bar.txt"
        grant_3 = "s3://*"
        grant_4 = "s3://bucket/foo*"
        self.assertEqual(self.access_grants_cache._AccessGrantsCache__process_matched_target(grant_1),
                         "s3://bucket/foo/bar")
        self.assertEqual(self.access_grants_cache._AccessGrantsCache__process_matched_target(grant_2),
                         "s3://bucket/foo/bar.txt")
        self.assertEqual(self.access_grants_cache._AccessGrantsCache__process_matched_target(grant_3),
                         "s3:/")
        self.assertEqual(self.access_grants_cache._AccessGrantsCache__process_matched_target(grant_4),
                         "s3://bucket/foo*")

    def test_cache_creation_with_invalid_cache_size(self):
        with self.assertRaises(IllegalArgumentException):
            AccessGrantsCache(cache_size=1000001)

    def test_cache_creation_with_invalid_duration(self):
        with self.assertRaises(IllegalArgumentException):
            AccessGrantsCache(duration=4099000)

    def test_cache_ttl(self):
        access_grants_cache = AccessGrantsCache(duration=2)
        requester_credentials = credentials.Credentials(access_key="access_key", secret_key="secret_key", token="token")
        key = CacheKey(requester_credentials, 'READ', "s3://bucket-name/prefixA")
        access_grants_cache._AccessGrantsCache__put_value_in_cache(key, self.access_grants_credentials)
        time.sleep(2)
        self.assertEqual(access_grants_cache._AccessGrantsCache__get_value_from_cache(key), None)

