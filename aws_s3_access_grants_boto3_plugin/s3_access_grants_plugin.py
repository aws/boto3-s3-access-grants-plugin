import botocore
from botocore import session
import logging
from aws_s3_access_grants_boto3_plugin.cache.access_denied_cache import AccessDeniedCache
from aws_s3_access_grants_boto3_plugin.cache.access_grants_cache import AccessGrantsCache
from aws_s3_access_grants_boto3_plugin.cache.cache_key import CacheKey
from aws_s3_access_grants_boto3_plugin.exceptions import IllegalArgumentException
from aws_s3_access_grants_boto3_plugin.operation_permissions import get_permission_for_s3_operation
from aws_s3_access_grants_boto3_plugin.cache.bucket_region_resolver_cache import BucketRegionResolverCache


class S3AccessGrantsPlugin:
    session = botocore.session.get_session()
    request = None
    sts_client = session.create_client('sts')
    access_denied_cache = AccessDeniedCache()
    access_grants_cache = AccessGrantsCache()
    bucket_region_cache = BucketRegionResolverCache()
    client_dict = {}
    internal_s3_client = session.create_client('s3')

    def __init__(self, s3_client, fallback_enabled):
        self.s3_client = s3_client
        self.fallback_enabled = fallback_enabled

    def register(self):
        self.s3_client.meta.events.register(
            'before-sign.s3', self.__get_access_grants_credentials
        )

    def __get_access_grants_credentials(self, operation_name, request, **kwargs):
        requester_credentials = self.s3_client._get_credentials()
        try:
            permission = get_permission_for_s3_operation(operation_name)
            s3_prefix = S3AccessGrantsPlugin.__get_s3_prefix(operation_name, request)
            cache_key = CacheKey(permission=permission, credentials=requester_credentials,
                                 s3_prefix="s3://" + s3_prefix)
            requester_account_id = self.sts_client.get_caller_identity()['Account']
            bucket_name = request.context['input_params']['Bucket']
            s3_control_client = self.__get_s3_control_client_for_region(bucket_name)
            request.context['signing']['credentials'] = self.__get_value_from_cache(cache_key, s3_control_client,
                                                                                    requester_account_id)
        except Exception as e:
            if self.__should_fallback_to_default_credentials_for_this_case(e):
                pass
            else:
                raise e

    def __should_fallback_to_default_credentials_for_this_case(self, e):
        if e.__class__.__name__ == 'UnsupportedOperationError':
            logging.debug(
                "Operation not supported by S3 access grants. Falling back to evaluate permission through policies.")
            return True
        if self.fallback_enabled:
            logging.debug("Fall back enabled on the plugin. Falling back to evaluate permission through policies.")
            return True
        return False

    @staticmethod
    def __get_s3_prefix(operation_name, request):
        s3_prefix = None
        if operation_name == 'DeleteObjects':
            bucket_name = request.context['input_params']['Bucket']
            prefixes = request.context['input_params']['Delete']['Objects']
            prefix_list = []
            for i in prefixes:
                prefix_list.append(i['Key'])
            s3_prefix = bucket_name + S3AccessGrantsPlugin.__get_common_prefix_for_multiple_prefixes(prefix_list)
            pass
        elif operation_name == 'CopyObject':
            destination_bucket_name = request.context['input_params']['Bucket']
            source_split = request.context['s3_redirect']['params']['CopySource'].split('/', 1)
            source_bucket = source_split[0]
            if source_bucket != destination_bucket_name:
                raise IllegalArgumentException("Source bucket and destination bucket must be the same.")
            prefix_list = [source_split[1], request.context['input_params']['Key']]
            s3_prefix = destination_bucket_name + S3AccessGrantsPlugin.__get_common_prefix_for_multiple_prefixes(prefix_list)
        else:
            s3_prefix = request.context['input_params']['Bucket']
            try:
                s3_prefix = s3_prefix + "/" + request.context['input_params']['Key']
            except KeyError:
                pass
        return s3_prefix

    @staticmethod
    def __get_common_prefix_for_multiple_prefixes(prefixes):
        if len(prefixes) == 0:
            return '/'
        common_ancestor = first_key = prefixes[0]
        last_prefix = ''
        for prefix in prefixes[1:]:
            while common_ancestor != "":
                if not prefix.startswith(common_ancestor):
                    last_index = common_ancestor.rfind('/')
                    if last_index == -1:
                        return "/"
                    last_prefix = common_ancestor[last_index + 1:]
                    common_ancestor = common_ancestor[:last_index]
                else:
                    break
        new_common_ancestor = common_ancestor + "/" + last_prefix
        for prefix in prefixes:
            while last_prefix != "":
                if not prefix.startswith(new_common_ancestor):
                    last_prefix = last_prefix[0:-1]
                    new_common_ancestor = common_ancestor + "/" + last_prefix
                else:
                    break
        if new_common_ancestor == first_key+"/":
            return "/" + first_key
        return "/" + new_common_ancestor

    def __get_s3_control_client_for_region(self, bucket_name):
        region = self.bucket_region_cache.resolve(self.internal_s3_client, bucket_name)
        s3_control_client = self.client_dict.get(region)
        if s3_control_client is None:
            s3_control_client = self.session.create_client('s3control', region_name=region)
            self.client_dict[region] = s3_control_client
        return s3_control_client

    def __get_value_from_cache(self, cache_key, s3_control_client, requester_account_id):
        access_denied_exception = self.access_denied_cache.get_value_from_cache(cache_key)
        if access_denied_exception is not None:
            logging.debug("Found cached Access Denied Exception.")
            raise access_denied_exception
        return self.access_grants_cache.get_credentials(s3_control_client, cache_key,
                                                        requester_account_id,
                                                        self.access_denied_cache)
