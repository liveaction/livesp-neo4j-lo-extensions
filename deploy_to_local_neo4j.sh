#!/usr/bin/env bash

mvn clean install && rm ~/applications/neo4j/plugins/* && tar xvfpz target/neo4j-lo-extensions-1.7-SNAPSHOT-plugin.tar.gz -C ~/applications/neo4j/plugins