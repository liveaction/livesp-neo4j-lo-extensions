#!/usr/bin/env bash

source ~/.back_bash_aliases

VERSION=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -v '\[' | grep -v "Downloaded:" | grep -v "Downloading:"`

mvn clean install && sudo rm -f ~/applications/neo4j/plugins/* && sudo mkdir -p ~/applications/neo4j/plugins && sudo tar xvfpz target/neo4j-lo-extensions-${VERSION}-plugin.tar.gz -C ~/applications/neo4j/plugins

neo4j_stop

neo4j_start
