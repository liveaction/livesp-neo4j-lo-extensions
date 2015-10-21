neo4j-lo-extensions
===================
[![Build Status](https://api.travis-ci.org/livingobjects/neo4j-lo-extensions.png)](https://travis-ci.org/livingobjects/neo4j-lo-extensions)

## Content

Contains 2 neo4j server extensions :

/warm-up : used to load all graph data in memory cache.

/load-csv : extension used to execute a LOAD CSV query using a remote CSV file that is uploaded.

## Load CSV

POST /load-csv

Requires a "Content-Type:multipart/mixed" with two body parts in this order:

- The cypher query with its parameters (json) :

```json
{
    "statement": "LOAD CSV FROM {file} AS csvFile\nCREATE (n:Node)",
    "parameters": {
        ...
    }
}
```

- The CSV file to upload and import

Example REST call using httpie :

```shell
curl -H "Content-Type:multipart/mixed" -F "content=@src/test/resources/query.json" -F "content=@src/test/resources/import.csv" -X POST http://localhost:7474/unmanaged/load-csv -i -v
```

Returns :

When execution succeeds :

```json
{"stats":{"nodesDeleted":0,"relationshipsCreated":0,"relationshipsDeleted":0,"propertiesSet":0,"labelsAdded":3,"labelsRemoved":0,"indexesAdded":0,"indexesRemoved":0,"constraintsAdded":0,"nodesCreated":3,"constraintsRemoved":0,"deletedNodes":0,"deletedRelationships":0}}
```

When execution fails :

```json
{"error":{"code":"org.neo4j.kernel.impl.query.QueryExecutionKernelException","message":"Invalid input ''': expected whitespace, comment, WITH or FROM (line 1, column 10 (offset: 9))\n\"LOAD CSV 'file:/tmp/rep4588354198555724947tmp' AS csvFile\"\n          ^"}}
```