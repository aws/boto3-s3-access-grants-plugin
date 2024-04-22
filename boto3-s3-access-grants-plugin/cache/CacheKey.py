class CacheKey:
    def __init__(self, credentials, permission, s3_prefix):
        self.credentials = credentials
        self.permission = permission
        self.s3_prefix = s3_prefix

    def __eq__(self, other):
        return ((self.credentials.access_key, self.credentials.secret_key, self.permission, self.s3_prefix) ==
                (other.credentials.access_key, other.credentials.secret_key, other.permission, other.s3_prefix))

    def __hash__(self):
        return hash((self.credentials.access_key, self.credentials.secret_key, self.permission, self.s3_prefix))

    def set_permission(self, permission):
        self.permission = permission

    def set_s3_prefix(self, s3_prefix):
        self.s3_prefix = s3_prefix
