import unittest
import mock
from aws_s3_access_grants_boto3_plugin.exceptions import IllegalArgumentException
from aws_s3_access_grants_boto3_plugin.cache.account_id_resolver_cache import AccountIdResolverCache


class TestAccountIdResolverCache(unittest.TestCase):
    mock_s3_control_client = mock.Mock()

    # test to check if resolve method returns expected accountId
    def test_resolve_method(self):
        cache = AccountIdResolverCache()
        self.mock_s3_control_client.get_access_grants_instance_for_prefix.return_value = {
            'AccessGrantsInstanceArn': 'arn:aws:s3:us-east-2:987654321098:access-grants/default',
            'AccessGrantsInstanceId': 'abcdefghijklmnopqrstuvwxyz'
        }
        self.assertEqual(cache.resolve(self.mock_s3_control_client, "123456789012", "s3://bucketName/prefixA"),
                         "987654321098")

    # test to check if service call is made only once for every resolve call with the same bucket name
    @mock.patch('aws_s3_access_grants_boto3_plugin.cache.account_id_resolver_cache.AccountIdResolverCache._AccountIdResolverCache__resolve_from_service')
    def test_count_resolve_method(self, mocked_resolve_from_service):
        cache = AccountIdResolverCache()
        cache.resolve(self.mock_s3_control_client, "123456789012", "s3://bucketName/prefixA")
        cache.resolve(self.mock_s3_control_client, "123456789012", "s3://bucketName/prefixB")
        mocked_resolve_from_service.assert_called_once()

    # test to check if resolve_from_service method returns expected accountId
    def test_resolve_from_service_method(self):
        cache = AccountIdResolverCache()
        self.mock_s3_control_client.get_access_grants_instance_for_prefix.return_value = {
            'AccessGrantsInstanceArn': 'arn:aws:s3:us-east-2:987654321098:access-grants/default',
            'AccessGrantsInstanceId': 'abcdefghijklmnopqrstuvwxyz'
        }
        self.assertEqual(
            cache._AccountIdResolverCache__resolve_from_service(self.mock_s3_control_client, "123456789012", "s3://bucketName/prefixA"),
            "987654321098")

    # test to check if get_bucket_name method returns expected bucket name for s3 prefix
    def test_get_bucket_name(self):
        cache = AccountIdResolverCache()
        self.assertEqual(cache._AccountIdResolverCache__get_bucket_name("s3://bucketName/prefixA"), "bucketName")

    # test to check if init method of AccountIdResolverCache throws IllegalArgumentException for invalid cache size
    def test_cache_creation_with_invalid_cache_size(self):
        with self.assertRaises(IllegalArgumentException):
            AccountIdResolverCache(cache_size=1000001)

    # test to check if init method of AccountIdResolverCache throws IllegalArgumentException for invalid ttl
    def test_cache_creation_with_invalid_ttl(self):
        with self.assertRaises(IllegalArgumentException):
            AccountIdResolverCache(cache_ttl=2592001)
