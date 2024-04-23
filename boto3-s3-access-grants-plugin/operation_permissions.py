from types import MappingProxyType
from exceptions import UnsupportedOperationError

_s3_operation_to_permission_map = MappingProxyType(
  {
    "HEADOBJECT": "READ",
    "GETOBJECT": "READ",
    "GETOBJECTACL": "READ",
    "LISTMULTIPARTUPLOADS": "READ",
    "LISTOBJECTS": "READ",
    "LISTOBJECTSV2": "READ",
    "LISTOBJECTVERSIONS": "READ",
    "LISTPARTS": "READ",
    "PUTOBJECT": "WRITE",
    "PUTOBJECTACL": "WRITE",
    "DELETEOBJECT": "WRITE",
    "DELETEOBJECTS": "WRITE",
    "ABORTMULTIPARTUPLOAD": "WRITE",
    "CREATEMULTIPARTUPLOAD": "WRITE",
    "UPLOADPART": "WRITE",
    "COMPLETEMULTIPARTUPLOAD": "WRITE",
    "COPYOBJECT": "READWRITE",
    "HEADBUCKET": "READ"
  }
)

def get_permission_for_s3_operation(operation):
  permission = _s3_operation_to_permission_map.get(operation.upper())
  if permission is None:
    raise UnsupportedOperationError("Access Grants does not support the requested operation.")
  return permission
