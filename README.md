## Amazon S3 Access Grants plugin for boto3

Amazon S3 Access Grants Plugin provides the functionality to enable S3 customers to configure S3 ACCESS GRANTS as a permission layer on top of the S3 Clients.

S3 Access Grants is a feature from S3 that allows its customers to configure fine-grained access permissions for the data in their buckets.

---

### Installing the plugin 
Run this command to install the plugin.
``` 
pip install aws-s3-access-grants-boto3-plugin==<Latest_Version>
```
### Using the plugin
1. Create your S3 Client.
2. Create a S3AccessGrantsPlugin object and pass the S3 Client and fallback option during initialization.
3. Register the plugin.

```
import botocore.session
from aws_s3_access_grants_boto3_plugin.s3_access_grants_plugin import S3AccessGrantsPlugin

session = botocore.session.get_session()
s3_client = session.create_client('s3')
plugin = S3AccessGrantsPlugin(s3_client, fallback_enabled=True, customer_session=session)
plugin.register()
```

fallback_enabled takes in a boolean value. This option decides if we will fall back to the credentials set on the S3 Client by the user.
1. If fallback_enabled is set to True then we will fall back every time we are not able to get the credentials from Access Grants, no matter the reason.
2. If fallback_enabled option is set to False we will fall back only in case the operation/API is not supported by Access Grants.

customer_session is an optional parameter of type botocore.session.Session. This session will be used to create the internal sts, s3, and s3control clients. If no session is passed the default botocore session will be used to create these clients.

### Notes
* The plugin supports delete_objects API and copy_object API which S3 Access Grants does not implicitly support. For these APIs we get the common prefix of all the object keys and find their common ancestor. If you  have a grant present on the common ancestor, you will get Access Grants credentials based on that grant.
For copy_object API the source and destination buckets should be same, since a grant cannot give access to multiple buckets.

---
### Contributions
* See CONTRIBUTING.md
---

### License

This project is licensed under the Apache-2.0 License.

