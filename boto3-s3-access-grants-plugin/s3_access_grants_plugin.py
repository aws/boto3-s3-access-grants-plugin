import botocore
from botocore import session
import logging
from access_denied_cache import AccessDeniedCache
from access_grants_cache import AccessGrantsCache
from cache_key import CacheKey
from operation_permissions import get_permission_for_s3_operation


class S3AccessGrantsPlugin:
    session = botocore.session.get_session()
    request = None
    s3_control_client = session.create_client('s3control')
    sts_client = session.create_client('sts')
    access_denied_cache = AccessDeniedCache()
    access_grants_cache = AccessGrantsCache()

    def __init__(self, s3_client, fallback_enabled):
        self.s3_client = s3_client
        self.fallback_enabled = fallback_enabled

    def register(self):
        self.s3_client.meta.events.register(
            'before-sign.s3', self.provide_identity
        )

    def provide_identity(self, request, operation_name, **kwargs):
        self.request = request
        self.get_access_grants_credentials(operation_name)

    def __should_fallback_to_default_credentials_for_this_case(self, e):
        if self.fallback_enabled:
            logging.debug("Fall back enabled on the plugin! falling back to evaluate permission through policies!")
            return True
        if e.__class__.__name__ == 'UnsupportedOperationError':
            logging.debug("Operation not supported by S3 access grants! fall back to evaluate permission through policies!")
            return True
        return False

    def get_access_grants_credentials(self, operation_name):
        requester_credentials = self.s3_client._get_credentials()
        try:
            permission = get_permission_for_s3_operation(operation_name)
            s3_prefix = self.request.context['input_params']['Bucket']
            try:
                s3_prefix = s3_prefix + "/" + self.request.context['input_params']['Prefix']
            except KeyError:
                pass
            cache_key = CacheKey(permission=permission, credentials=requester_credentials,
                                 s3_prefix="s3://" + s3_prefix)
            requester_account_id = self.sts_client.get_caller_identity()['Account']
            self.request.context['signing']['credentials'] = self.__get_value_from_cache(cache_key, requester_account_id)
        except Exception as e:
            if self.__should_fallback_to_default_credentials_for_this_case(e):
                self.request.context['signing']['credentials'] = requester_credentials
            else:
                raise e

    def __get_value_from_cache(self, cache_key, requester_account_id):
        access_denied_exception = self.access_denied_cache.get_value_from_cache(cache_key)
        if access_denied_exception is not None:
            logging.debug("Found cached Access Denied Exception")
            raise access_denied_exception
        return self.access_grants_cache.get_credentials(self.s3_control_client, cache_key,
                                                                       requester_account_id,
                                                                       self.access_denied_cache)