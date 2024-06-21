import unittest
import mock
from botocore.exceptions import ClientError
from aws_s3_access_grants_boto3_plugin.cache.bucket_region_resolver_cache import BucketRegionResolverCache


class TestBucketRegionResolverCache(unittest.TestCase):
    s3_client = mock.Mock()

    def __reset_mock(self):
        self.s3_client.reset_mock(return_value=True, side_effect=True)

    def test_resolve(self):
        cache = BucketRegionResolverCache()
        expectedRegion = 'us-east-2'
        self.s3_client.head_bucket.return_value = {
            'BucketRegion': expectedRegion,
        }
        self.assertEqual(cache.resolve(self.s3_client, 'fakebucket'), expectedRegion)
        self.__reset_mock()

    @mock.patch('aws_s3_access_grants_boto3_plugin.cache.bucket_region_resolver_cache.BucketRegionResolverCache._BucketRegionResolverCache__resolve_from_service')
    def test_resolve_caches_response(self, mocked_resolve_from_service):
        cache = BucketRegionResolverCache()
        cache.resolve(self.s3_client, 'fakebucket')
        cache.resolve(self.s3_client, 'fakebucket')
        mocked_resolve_from_service.assert_called_once()

    def test_service_redirect_with_header(self):
        cache = BucketRegionResolverCache()
        expectedRegion = 'us-east-2'
        self.s3_client.head_bucket.side_effect = ClientError(
            operation_name='head_bucket',
            error_response={
                'ResponseMetadata': {
                    'HTTPStatusCode': 301,
                    'HTTPHeaders': {
                        'x-amz-bucket-region': expectedRegion,  # expected region header
                    }
                }
            }
        )
        self.assertEqual(cache.resolve(self.s3_client, 'fakebucket'), expectedRegion)
        self.__reset_mock()

    def test_service_redirect_without_header(self):
        cache = BucketRegionResolverCache()
        self.s3_client.head_bucket.side_effect = ClientError(
            operation_name='head_bucket',
            error_response={
                'Error': {
                    'Message': 'Redirect'
                },
                'ResponseMetadata': {
                    'HTTPStatusCode': 301,
                    'HTTPHeaders': {
                        'x-amz-bucket-region': None,
                    }
                }
            }
        )
        with self.assertRaises(ClientError) as e:
            cache.resolve(self.s3_client, 'fakebucket')
            self.assertEqual(e.response['Error']['Message'], 'Redirect')
        self.__reset_mock()

    def test_bucket_not_exists(self):
        cache = BucketRegionResolverCache()
        self.s3_client.head_bucket.side_effect = ClientError(
            operation_name='head_bucket',
            error_response={
                'Error': {
                    'Message': 'Bucket does not exist'
                },
                'ResponseMetadata': {
                    'HTTPStatusCode': 404,
                    'HTTPHeaders': {}
                }
            }
        )
        with self.assertRaises(ClientError) as e:
            cache.resolve(self.s3_client, 'fakebucket')
            self.assertEqual(e.response['Error']['Message'], 'Bucket does not exist')
        self.__reset_mock()
