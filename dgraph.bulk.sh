#!/bin/bash

if [[ $# -lt 3 ]]
then
	echo "Provide path to the data, where to bulk load the files,"
	echo "the schema and rdf files in data as mounted to /data."
	exit 1
fi

data=$(cd "$1"; pwd)
bulk=$(cd "$2"; pwd)
schema="$3"

cp dgraph.bulk.load.sh $bulk/
docker run --rm -it -v "$data:/data" -v "$bulk:/dgraph" dgraph/dgraph:v21.03.0 /dgraph/dgraph.bulk.load.sh "$schema" "${@:4}"

