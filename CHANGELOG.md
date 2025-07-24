## 1.3.0 2025-07-23
### Features
* Bumping up the minimum python language and dependency requirements for the plugin
* fallback_enabled setting can now be passed via environment variables
---

## 1.2.0 2024-09-20
### Features
* Increased the account_id_resolver_cache and bucket_region_resolver_cache ttl to 1 hour
---

## 1.1.0 2024-09-04
### Features
* You can now pass a session while initiating the plugin. That session will be used to create s3, sts, and s3control clients internally.
### Bugfix
* Now you can get the prefix from ListObjects, ListObjectsV2, ListObjectVersions, and ListMultipartUploads  and you can call getDataAccess on that prefix.
* Refactored private method names.
---

## 1.0.1 2024-07-22
### Bugfix
* Fixed dependency issue. Now you don't have to install dependencies separately.
---

## 1.0.0 2024-06-21
### Features
* Released the first version of the plugin.
---
