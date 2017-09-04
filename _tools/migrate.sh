#!/usr/bin/env bash

temp_file=$(mktemp --tmpdir=/dev/shm/)

trap "rm -rf ${temp_file}" EXIT

curl -X POST "http://localhost:7474/unmanaged/migration/v2_00_0"
echo ""

status=0;
while [ ${status} -lt 200 ]; do
    sleep 2;
    status=$(curl -s -o ${temp_file} -w "%{http_code}" "http://localhost:7474/unmanaged/migration/status")
    cat ${temp_file}
done

