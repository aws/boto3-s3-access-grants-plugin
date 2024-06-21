from cacheout import Cache
import logging
from botocore.exceptions import ClientError
from aws_s3_access_grants_boto3_plugin.cache.account_id_resolver_cache import AccountIdResolverCache
from aws_s3_access_grants_boto3_plugin.cache.cache_key import CacheKey
from aws_s3_access_grants_boto3_plugin.exceptions import IllegalArgumentException

DEFAULT_ACCESS_GRANTS_CACHE_SIZE = 30000
MAX_LIMIT_ACCESS_GRANTS_CACHE_SIZE = 1000000
GET_DATA_ACCESS_DURATION = 1 * 60 * 60  # 1 hour
MAX_GET_DATA_ACCESS_DURATION = 12 * 60 * 60  # 12 hours
CACHE_EXPIRATION_TIME_PERCENTAGE = 90


class AccessGrantsCache:
    access_grants_cache = None
    account_id_resolver_cache = AccountIdResolverCache()

    def __init__(self, cache_size=DEFAULT_ACCESS_GRANTS_CACHE_SIZE,
                 duration=GET_DATA_ACCESS_DURATION):
        self.cache_size = cache_size
        self.duration = duration
        self.cache_ttl = (duration * CACHE_EXPIRATION_TIME_PERCENTAGE) / 100

        if self.cache_size > MAX_LIMIT_ACCESS_GRANTS_CACHE_SIZE:
            raise IllegalArgumentException(
                "Max cache size should be less than or equal to " + str(MAX_LIMIT_ACCESS_GRANTS_CACHE_SIZE))

        if self.duration > GET_DATA_ACCESS_DURATION:
            raise IllegalArgumentException("Maximum duration should be less than or equal to " + str(
                GET_DATA_ACCESS_DURATION))

        self.access_grants_cache = Cache(maxsize=self.cache_size, ttl=self.cache_ttl)

    #  This is for grants of type "s3://bucket/prefix/*"
    def __search_credentials_at_prefix_level(self, cache_key):

        prefix = cache_key.s3_prefix
        while prefix != "s3:":
            cache_key = CacheKey(cache_key=cache_key, s3_prefix=prefix)
            cache_value = self.access_grants_cache.get(cache_key)
            if cache_value is not None:
                logging.debug("Successfully retrieved credentials from cache.")
                return cache_value
            prefix = prefix.rsplit('/', 1)[0]
        return None

    # This is for grants of type "s3://bucket/prefix*"
    def __search_credentials_at_character_level(self, cache_key):
        prefix = cache_key.s3_prefix
        while prefix != "s3://":
            cache_key = CacheKey(cache_key=cache_key, s3_prefix=prefix + "*")
            cache_value = self.access_grants_cache.get(cache_key)
            if cache_value is not None:
                logging.debug("Successfully retrieved credentials from cache.")
                return cache_value
            prefix = prefix[:-1]
        return None

    def __get_credentials_from_service(self, s3_control_client, cache_key, account_id):
        if s3_control_client is None:
            raise IllegalArgumentException("S3 Control Client should not be null")
        bucket_owner_account_id = self.account_id_resolver_cache.resolve(s3_control_client, account_id,
                                                                         cache_key.s3_prefix)
        logging.debug((
                "Fetching credentials from Access Grants for accountId: " + bucket_owner_account_id + ", s3Prefix: " + cache_key.s3_prefix +
                ", permission: " + cache_key.permission + ", privilege: " + "DEFAULT"))
        return s3_control_client.get_data_access(AccountId=bucket_owner_account_id, Target=cache_key.s3_prefix,
                                                 Permission=cache_key.permission, Privilege='Default')

    # This method removes '/*' from matchedGrantTarget if present.
    # This helps us differentiate between grants of type "s3://bucket/prefix/*" and "s3://bucket/prefix*".
    @staticmethod
    def __process_matched_target(matched_grant_target):
        if matched_grant_target.endswith("/*"):
            return matched_grant_target[:-2]
        return matched_grant_target

    def get_credentials(self, s3_control_client, cache_key, account_id, access_denied_cache):
        logging.debug("Fetching credentials from Access Grants for s3Prefix: " + cache_key.s3_prefix)
        credentials = self.__search_credentials_at_prefix_level(cache_key)
        if credentials is None and (cache_key.permission == "READ" or cache_key.permission == "WRITE"):
            credentials = self.__search_credentials_at_prefix_level(
                CacheKey(permission="READWRITE", cache_key=cache_key))
        if credentials is None:
            credentials = self.__search_credentials_at_character_level(cache_key)
        if credentials is None and (cache_key.permission == "READ" or cache_key.permission == "WRITE"):
            credentials = self.__search_credentials_at_character_level(
                CacheKey(permission="READWRITE", cache_key=cache_key))
        if credentials is None:
            logging.debug("Credentials not available in the cache. Fetching credentials from Access Grants service.")
            try:
                response = self.__get_credentials_from_service(s3_control_client, cache_key, account_id)
                credentials = response["Credentials"]
                matched_grant_target = response["MatchedGrantTarget"]
                if matched_grant_target.endswith("*"):  # we do not cache object level grants
                    logging.debug("Caching the credentials for s3Prefix:" + matched_grant_target
                                  + " and permission: " + cache_key.permission)
                    self.access_grants_cache.set(
                        CacheKey(s3_prefix=AccessGrantsCache.__process_matched_target(matched_grant_target),
                                 cache_key=cache_key), credentials)
                logging.debug("Successfully retrieved credentials from Access Grants service.")
            except ClientError as e:
                logging.debug(
                    "Exception occurred while fetching the credentials from Access Grants: " + e.response["Error"][
                        "Message"])
                if e.response["Error"]["Code"] == "AccessDenied":
                    logging.debug("Caching the Access Denied request.")
                    access_denied_cache.put_value_in_cache(cache_key, e)
                raise e
        return credentials

    def __put_value_in_cache(self, cache_key, value):
        return self.access_grants_cache.set(cache_key, value)

    def __get_value_from_cache(self, cache_key):
        return self.access_grants_cache.get(cache_key)
