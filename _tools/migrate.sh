#!/usr/bin/env bash

if [ -z "$1" ]
  then
    echo "You most give the host and port for Neo4j Server (Ex: `migrate.sh localhost:7474`"
fi

host_port=$1
temp_file=$(mktemp --tmpdir=/dev/shm/)

trap "rm -rf ${temp_file}" EXIT

status=$(curl -s -o ${temp_file} -w "%{http_code}" -X POST "http://${host_port}/unmanaged/migration/v2_00_0")
if [ $status -eq 405 ]
then
    echo "Update Neo4j extension to the last version (>= 1.7)"
    exit 1
else
    cat ${temp_file}
    echo ""
fi

status=0;
while [ ${status} -lt 200 ]; do
    sleep 2;
    status=$(curl -s -o ${temp_file} -w "%{http_code}" "http://${host_port}/unmanaged/migration/status")
    cat ${temp_file}
done

