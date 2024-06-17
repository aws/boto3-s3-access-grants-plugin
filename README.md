## Amazon S3 Access Grants plugin for boto3

Amazon S3 Access Grants Plugin provides the functionality to enable S3 customers to configure S3 ACCESS GRANTS as a permission layer on top of the S3 Clients.

S3 Access Grants is a feature from S3 that allows its customers to configure fine-grained access permissions for the data in their buckets.

---

### Installing the plugin 
Run this command to install the plugin.
``` 
python3 -m pip install "s3-access-grants-plugin==<Latest_Version>"
```

### Using the plugin
1. Create your S3 Client.
2. Create a S3AccessGrantsPlugin object and pass the S3 Client and fallback option during initialization.
3. Register the plugin.

```
session = botocore.session.get_session()
s3_client = newsession.create_client('s3')
plugin = S3AccessGrantsPlugin(s3_client, fallback_enabled=True)
plugin.register()
```

fallback_enabled takes in a boolean value. This option decides if we will fall back to the credentials set on the S3 Client by the user.
1. If fallback_enabled is set to True then we will fall back every time we are not able to get the credentials from Access Grants, no matter the reason.
2. If fallback_enabled option is set to False we will fall back only in case the operation/API is not supported by Access Grants.

---
### Contributions

* Use [GitHub flow](https://docs.github.com/en/get-started/quickstart/github-flow) to commit/review/collaborate on changes
* After a PR is approved/merged, please delete the PR branch both remotely and locally


---

### License

This project is licensed under the Apache-2.0 License.

