import botocore
from botocore import session
from botocore import config
from botocore import credentials
from botocore.utils import create_nested_client
import logging
import os
from aws_s3_access_grants_boto3_plugin.cache.access_denied_cache import AccessDeniedCache
from aws_s3_access_grants_boto3_plugin.cache.access_grants_cache import AccessGrantsCache
from aws_s3_access_grants_boto3_plugin.cache.cache_key import CacheKey
from aws_s3_access_grants_boto3_plugin.exceptions import IllegalArgumentException
from aws_s3_access_grants_boto3_plugin.operation_permissions import get_permission_for_s3_operation
from aws_s3_access_grants_boto3_plugin.cache.bucket_region_resolver_cache import BucketRegionResolverCache


class S3AccessGrantsPlugin:
    request = None
    access_denied_cache = AccessDeniedCache()
    access_grants_cache = AccessGrantsCache()
    bucket_region_cache = BucketRegionResolverCache()
    client_dict = {}
    session_config = botocore.config.Config(user_agent="aws_s3_access_grants_boto3_plugin")

    def __init__(self, s3_client, fallback_enabled=None, customer_session=None):

        self.s3_client = s3_client

        # Handle fallback_enabled parameter with environment variable fallback
        if fallback_enabled is not None:
            self.fallback_enabled = fallback_enabled
        else:
            self.fallback_enabled = True

        if isinstance(customer_session, botocore.session.Session):
            self.session = customer_session
            self.sts_client = create_nested_client(self.session, 'sts', config=self.session_config)
            self.internal_s3_client = create_nested_client(self.session, 's3', config=self.session_config)
        elif customer_session is None:  # Customer has not set session explicitly, so we use default botocore session
            self.session = botocore.session.get_session()
            self.sts_client = create_nested_client(self.session, 'sts', config=self.session_config)
            self.internal_s3_client = create_nested_client(self.session, 's3', config=self.session_config)
        else:
            raise IllegalArgumentException("customer_session must be type of botocore.session")

    def register(self):
        self.s3_client.meta.events.register(
            'before-sign.s3', self._get_access_grants_credentials
        )

    def _get_access_grants_credentials(self, operation_name, request, **kwargs):
        requester_credentials = self.s3_client._get_credentials()
        try:
            permission = get_permission_for_s3_operation(operation_name)
            s3_prefix = self._get_s3_prefix(operation_name, request)
            cache_key = CacheKey(permission=permission, credentials=requester_credentials,
                                 s3_prefix="s3://" + s3_prefix)
            requester_account_id = self.sts_client.get_caller_identity()['Account']
            bucket_name = request.context['input_params']['Bucket']
            s3_control_client = self._get_s3_control_client_for_region(bucket_name)
            s3ag_credentials = self._get_value_from_cache(cache_key, s3_control_client, requester_account_id)
            request.context['signing']['request_credentials'] = botocore.credentials.Credentials(access_key=s3ag_credentials['AccessKeyId'],
                                                                                                 secret_key=s3ag_credentials['SecretAccessKey'],
                                                                                                 token=s3ag_credentials['SessionToken'])

        except Exception as e:
            if self._should_fallback_to_default_credentials_for_this_case(e):
                pass
            else:
                raise e

    def _should_fallback_to_default_credentials_for_this_case(self, e):
        if e.__class__.__name__ == 'UnsupportedOperationError':
            logging.debug(
                "Operation not supported by S3 access grants. Falling back to evaluate permission through policies.")
            return True
        if self.fallback_enabled:
            logging.debug("Fall back enabled on the plugin. Falling back to evaluate permission through policies.")
            return True
        return False

    def _get_s3_prefix(self, operation_name, request):
        if operation_name == 'DeleteObjects':
            bucket_name = request.context['input_params']['Bucket']
            prefixes = request.context['input_params']['Delete']['Objects']
            prefix_list = []
            for i in prefixes:
                prefix_list.append(i['Key'])
            s3_prefix = bucket_name + self._get_common_prefix_for_multiple_prefixes(prefix_list)
            pass
        elif operation_name == 'CopyObject':
            destination_bucket_name = request.context['input_params']['Bucket']
            source_split = request.context['s3_redirect']['params']['CopySource'].split('/', 1)
            source_bucket = source_split[0]
            if source_bucket != destination_bucket_name:
                raise IllegalArgumentException("Source bucket and destination bucket must be the same.")
            prefix_list = [source_split[1], request.context['input_params']['Key']]
            s3_prefix = destination_bucket_name + self._get_common_prefix_for_multiple_prefixes(prefix_list)
        elif (operation_name == 'ListObjectsV2' or
              operation_name == 'ListObjects' or
              operation_name == 'ListObjectVersions' or
              operation_name == 'ListMultipartUploads'):
            s3_prefix = request.context['input_params']['Bucket']
            try:
                s3_prefix = s3_prefix + "/" + request.context['input_params']['Prefix']
            except KeyError:
                pass
        else:
            s3_prefix = request.context['input_params']['Bucket']
            try:
                s3_prefix = s3_prefix + "/" + request.context['input_params']['Key']
            except KeyError:
                pass
        return s3_prefix

    def _get_common_prefix_for_multiple_prefixes(self, prefixes):
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
        if new_common_ancestor == first_key + "/":
            return "/" + first_key
        return "/" + new_common_ancestor

    def _get_s3_control_client_for_region(self, bucket_name):
        region = self.bucket_region_cache.resolve(self.internal_s3_client, bucket_name)
        s3_control_client = self.client_dict.get(region)
        if s3_control_client is None:
            s3_control_client = create_nested_client(self.session, 's3control', region_name=region,
                                                           config=self.session_config)
            self.client_dict[region] = s3_control_client
        return s3_control_client

    def _get_value_from_cache(self, cache_key, s3_control_client, requester_account_id):
        access_denied_exception = self.access_denied_cache.get_value_from_cache(cache_key)
        if access_denied_exception is not None:
            logging.debug("Found cached Access Denied Exception.")
            raise access_denied_exception
        return self.access_grants_cache.get_credentials(s3_control_client, cache_key,
                                                        requester_account_id,
                                                        self.access_denied_cache)


def initialize_client_plugin(client):
    """
    Initialize and register the S3 Access Grants plugin for the given S3 client.
    This method is considered internal and subject to abrupt breaking changes without prior notice.  Please do not use it directly.
    """
    if not is_valid_boto3_s3_client(client):
        return
    plugin = S3AccessGrantsPlugin(client, fallback_enabled=True)
    plugin.register()

def is_valid_boto3_s3_client(client):
    """
    Validates that the provided s3_client is a valid boto3 S3 client instance.

    Args:
        s3_client: The client to validate

    Returns:
        bool: True if valid boto3 S3 client, False otherwise
    """
    return (
            isinstance(client, botocore.client.BaseClient)
            and client.meta.service_model.service_id.lower() == "s3"
        )
