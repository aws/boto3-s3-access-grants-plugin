from aws_s3_access_grants_boto3_plugin.exceptions import IllegalArgumentException


class CacheKey:
    def __init__(self, credentials=None, permission=None, s3_prefix=None, cache_key=None):
        if cache_key is None:
            self.credentials = credentials
            self.permission = permission
            self.s3_prefix = s3_prefix
        else:
            if permission is not None:
                self.credentials = cache_key.credentials
                self.s3_prefix = cache_key.s3_prefix
                self.permission = permission
            elif s3_prefix is not None:
                self.credentials = cache_key.credentials
                self.permission = cache_key.permission
                self.s3_prefix = s3_prefix

        if self.credentials is None or self.permission is None or self.s3_prefix is None:
            raise IllegalArgumentException("Credentials, permission, and s3_prefix must be provided")

    def __eq__(self, other):
        return ((self.credentials.access_key, self.credentials.secret_key, self.permission, self.s3_prefix) ==
                (other.credentials.access_key, other.credentials.secret_key, other.permission, other.s3_prefix))

    def __hash__(self):
        return hash((self.credentials.access_key, self.credentials.secret_key, self.permission, self.s3_prefix))
