# Changelog

## 1.5 _(2017-01-30)_
* New SchemaTemplateExtension based on XML template file
* Refactor model subpackages
* fix(memdexPath): Fix connerie ! do not remove public for MemdexPathWithRealm
* feat(memdexPath): Add specializer to MemdexPath attribute when needed
* fix(memdexPath): Fix error when multiple Attribute with different _type have the same name
* fix(pom): Fix plugin gpg version
* fix(pom): Fix parent for compilation problems
* fix(pom): Fix dependency for jetty-servlets

## 1.4 _(2016-11-28)_
* Add MemdexPathExtension (/memdexpath)
* Add CustomerSchemaExtension (/schema)
* feat(export): Add gzip filter output support
* feat(export): add exportTags parameter
* feat(export): sort property name in csv header
* feat(import): properties of all type are set to null when CSV value is empty
* fix(import): set line in error when orphan element in the line doesn't exist
* fix(iwan): Fix CrossAttribute links on existing CPE without cpe parents
* fix(import): fix import of null properties
* fix(import): properties of all type are set to null when CSV value is empty
* fix(export): optimize lineages

## 1.3.4 _(2016-10-10)_
* fix(load-csv): support null values for non string properties

## 1.3.3 _(2016-10-10)_
* fix(load-csv): import number values as correct type : int, long, float or double

## 1.3.2 _(2016-09-23)_
* fix(load-csv): support null values for non string properties

## 1.3.1 _(2016-07-20)_

* Add tar.gz assembly
* Add error log when CrossAttribute link definition doesn't exist
* Create CrossAttribute links according to meta schema

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
