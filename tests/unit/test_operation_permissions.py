import unittest

from aws_s3_access_grants_boto3_plugin.operation_permissions import get_permission_for_s3_operation
from aws_s3_access_grants_boto3_plugin.exceptions import UnsupportedOperationError


class TestOperationPermissionMapper(unittest.TestCase):
    def test_lowercase_operation_name(self):
        lowercase_operation = "getobject"
        permission = get_permission_for_s3_operation(lowercase_operation)
        self.assertEqual("READ", permission)

    def test_not_supported(self):
        operation = "DELETE_ACCOUNT"  # obviously not a real operation
        with self.assertRaises(UnsupportedOperationError):
            get_permission_for_s3_operation(operation)
