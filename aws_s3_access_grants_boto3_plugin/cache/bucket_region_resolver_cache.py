from cacheout import Cache
from botocore.exceptions import ClientError
import logging

DEFAULT_BUCKET_REGION_CACHE_SIZE = 1000
DEFAULT_BUCKET_REGION_CACHE_TTL = 5 * 60  # 5 minutes


class BucketRegionResolverCache:
    bucket_region_resolver_cache = None

    def __init__(self,
                 cache_size=DEFAULT_BUCKET_REGION_CACHE_SIZE,
                 cache_ttl=DEFAULT_BUCKET_REGION_CACHE_TTL):
        self.cache_size = cache_size
        self.cache_ttl = cache_ttl

        self.bucket_region_resolver_cache = Cache(maxsize=self.cache_size, ttl=self.cache_ttl)

    @staticmethod
    def __resolve_from_service(s3_client, bucket):
        try:
            head_bucket_response = s3_client.head_bucket(Bucket=bucket)
            resolved_region = head_bucket_response['BucketRegion']
        except ClientError as e:
            logging.debug("Client error when calling head bucket. Attempting to get region from request headers")
            # Try to get region from response header
            if (e.response and
                    e.response['ResponseMetadata'] and
                    e.response['ResponseMetadata']['HTTPHeaders'] and
                    e.response['ResponseMetadata']['HTTPHeaders']['x-amz-bucket-region']):
                resolved_region = e.response['ResponseMetadata']['HTTPHeaders']['x-amz-bucket-region']
            else:
                raise e
        return resolved_region

    def resolve(self, s3_client, bucket):
        bucket_region = self.bucket_region_resolver_cache.get(bucket)
        if bucket_region is None:
            logging.debug(f"Region for bucket \"{bucket}\" not available in cache. Fetching region from service")
            bucket_region = BucketRegionResolverCache.__resolve_from_service(s3_client, bucket)
            self.bucket_region_resolver_cache.set(bucket, bucket_region)
        return bucket_region
