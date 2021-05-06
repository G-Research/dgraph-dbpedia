#!/bin/bash

if [[ $# -lt 3 ]]
then
	echo "Provide path to the data, where to bulk load the files,"
	echo "the schema and rdf files in data as mounted to /data."
	exit 1
fi

mkdir -p "$2"
data=$(cd "$1"; pwd)
bulk=$(cd "$2"; pwd)
schema="$3"

cp dgraph.bulk.load.sh $bulk/
docker run --rm -it -v "$data:/data" -v "$bulk:/dgraph" dgraph/dgraph:v20.11.0-rc1-142-g79f6e8edc /dgraph/dgraph.bulk.load.sh "$schema" "${@:4}"

