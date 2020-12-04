#!/bin/bash

if [[ $# -lt 3 ]]
then
	echo "Provide path to the data, where to bulk load the files,"
	echo "and the files to load relative to the given data path"
	exit 1
fi

data=$(cd "$1"; pwd)
bulk=$(cd "$2"; pwd)
schema=$(cd "$3"; pwd)

mkdir -p $bulk
cp dgraph.bulk.load.sh $bulk/
docker run --rm -it -v "$data:/data" -v "$bulk:/dgraph" dgraph/dgraph:v20.11.0-rc1-142-g79f6e8edc /dgraph/dgraph.bulk.load.sh "$schema" "${@:4}"

