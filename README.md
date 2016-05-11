neo4j-lo-extensions
===================
[![Build Status](https://api.travis-ci.org/livingobjects/neo4j-lo-extensions.png)](https://travis-ci.org/livingobjects/neo4j-lo-extensions)

## Content

Contains 2 neo4j server extensions :

* `/warm-up` : used to load all graph data in memory cache.
* `/load-csv` : extension used to execute a LOAD CSV query using a remote CSV file that is uploaded.

## Load CSV

`POST /load-csv`

Requires a "Content-Type:multipart/mixed"

### The CSV header
This extension allow importing CSV files into existent neo4j database. The first line is the CSV header and is mandatory.

Each column header must be formatted as follow `<attribute>.<name>(:<type>([]))`. Ex: `neType:interface.id:NUMBER`. 

The type is optional, if not present, the default type is STRING. The possible type are :
 * STRING
 * NUMBER
 * BOOLEAN

If `[]` follow the type, the property value is defined as an array.

For **CrossAttribute** properties the `<attribute>` is formatted as follow `(<fromAttribute>»<toAttribute)`. Ex: 
`(neType:interface»neType:cos).id:NUMBER`. Be aware that a `tag` property must be present in header for each attribute
specified in the CrossAttribute field.

### The result

The request returns a json file:

- When execution succeeds (code 200) :

```json
{
  "stats": {
    "nodes_deleted": 0,
    "relationships_created": 0,
    "relationships_deleted": 0,
    "properties_set": 0,
    "labels_added": 2,
    "labels_removed": 0,
    "indexes_added": 0,
    "indexes_removed": 0,
    "constraints_added": 0,
    "nodes_created": 2,
    "constraints_removed": 0,
    "deleted_nodes": 0,
    "deleted_relationships": 0
  }
}
```


## How to use:

Copy neo4j-lo-extensions-*.jar file to neo4j plugins folder. Copy Guava and Metrics dependencies.

Edit neo4j-server.properties to setup extensions base url:

```properties
org.neo4j.server.thirdparty_jaxrs_classes=com.livingobjects.neo4j=/unmanaged
```

## To build and deploy to your internal repository :
 
```shell
mvn deploy -DaltDeploymentRepository=nexus::default::http://XXX.XXX.XXX.XXX:8081/nexus/content/repositories/releases
```