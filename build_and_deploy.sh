#!/bin/bash

mvn clean install &&
    rm ~/applications/neo4j/plugins/* &&
    tar xvfpz target/neo4j-lo-extensions-1.4-SNAPSHOT-plugin.tar.gz -C ~/applications/neo4j/plugins/ &&
    /home/lo/applications/neo4j/bin/neo4j stop &&
    /home/lo/applications/neo4j/bin/neo4j start

