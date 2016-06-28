# Changelog

## 1.3 _(2016-06-28)_

* Refactor extension by using neo4j API instead of LOAD CSV. This improve performance drastically.
* Allow import array properties
* Allow import CrossAttribute properties
* `a5af545`: feat(loader): when a propery is null, the property is not modified instead of being set to null
* `0c59ad4`: Use InvalidScopeException instead of IllegalArgumentException
* `200824c`: feat(IWanTopologyLoader): scope node is required and must exists. Deal with empty tags.
* `d899ed8`: fix(iwan): Fix COS importation issue
* `40f989c`: feat(iwan): Add scope to the overridable element
* `40f989c`: feat(iwan): Add scope to the overridable element
* `c53dcd1`: fix(iwan): Fix extension to allow creation of all type of NE

## 1.2

* Set logs to debug.
* Update  to neo4j 2.3.3.

## 1.1

* Use underscores instead of camelCase format for json stats response.

## 1.0

* Initial version
