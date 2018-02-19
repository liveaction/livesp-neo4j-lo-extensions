#!/usr/bin/env bash

VERSION=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -v '\[' | grep -v "Downloaded:" | grep -v "Downloading:"`

mvn clean deploy -U && rm -f ~/workspace/github/docker-common/neo4j/neo4j/plugins/* && tar xvfpz target/neo4j-lo-extensions-${VERSION}-plugin.tar.gz -C ~/workspace/github/docker-common/neo4j/neo4j/plugins/