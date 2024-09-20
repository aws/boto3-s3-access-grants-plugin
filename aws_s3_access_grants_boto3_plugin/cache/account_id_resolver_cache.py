from cacheout import Cache
from aws_s3_access_grants_boto3_plugin.exceptions import IllegalArgumentException

DEFAULT_ACCOUNT_ID_CACHE_SIZE = 1000
DEFAULT_TTL = 60 * 60
MAX_LIMIT_ACCOUNT_ID_CACHE_SIZE = 1000000
MAX_LIMIT_TTL = 2592000


class AccountIdResolverCache:
    account_id_resolver_cache = None

    def __init__(self, cache_size=DEFAULT_ACCOUNT_ID_CACHE_SIZE,
                 cache_ttl=DEFAULT_TTL):
        self.cache_size = cache_size
        self.cache_ttl = cache_ttl

        if self.cache_size > MAX_LIMIT_ACCOUNT_ID_CACHE_SIZE:
            raise IllegalArgumentException(
                "Max cache size should be less than or equal to " + str(MAX_LIMIT_ACCOUNT_ID_CACHE_SIZE))

        if self.cache_ttl > MAX_LIMIT_TTL:
            raise IllegalArgumentException("Maximum ttl should be less than or equal to " + str(
                MAX_LIMIT_TTL))

        self.account_id_resolver_cache = Cache(maxsize=self.cache_size, ttl=cache_ttl)

    def _get_bucket_name(self, s3_prefix):
        split_prefix = s3_prefix.split("/")
        return split_prefix[2]

    def _resolve_from_service(self, s3_control_client, account_id, s3_prefix):
        access_grants_instance_for_prefix = s3_control_client.get_access_grants_instance_for_prefix(
            AccountId=account_id, S3Prefix=s3_prefix)
        access_grants_instance_arn = access_grants_instance_for_prefix['AccessGrantsInstanceArn']
        return access_grants_instance_arn.split(":")[4]

    def resolve(self, s3_control_client, requester_account_id, s3_prefix):
        bucket_name = self._get_bucket_name(s3_prefix)
        account_id = self.account_id_resolver_cache.get(bucket_name)
        if account_id is None:
            account_id = self._resolve_from_service(s3_control_client, requester_account_id, s3_prefix)
            self.account_id_resolver_cache.set(bucket_name, account_id)
        return account_id
