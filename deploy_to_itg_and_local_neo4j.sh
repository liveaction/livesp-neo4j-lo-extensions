#!/usr/bin/env bash

VERSION=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -v '\[' | grep -v "Downloaded:" | grep -v "Downloading:"`

mvn clean install && rm -f ~/applications/neo4j/plugins/* && tar xvfpz target/neo4j-lo-extensions-${VERSION}-plugin.tar.gz -C ~/applications/neo4j/plugins

ssh itg_long "mkdir -p /tmp/neo4j_ext && rm -f /tmp/neo4j_ext/*"
scp ~/applications/neo4j/plugins/* itg_long:/tmp/neo4j_ext

ssh itg_long << EOF
    docker exec -t neo4j sh -c "rm -Rf /tmp/neo4j_ext"
    docker cp /tmp/neo4j_ext neo4j:/tmp
    docker exec -t neo4j sh -c "rm -f /opt/neo4j/plugins/*"
    docker exec -t neo4j sh -c "cp /tmp/neo4j_ext/* /opt/neo4j/plugins"
    docker restart neo4j
EOF

/home/lo/applications/neo4j/bin/neo4j stop

/home/lo/applications/neo4j/bin/neo4j start