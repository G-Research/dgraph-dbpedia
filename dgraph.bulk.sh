#!/bin/bash

if [[ $# -lt 3 ]]
then
	echo "Provide path to the data, where to bulk load the files,"
	echo "and the files to load relative to the given data path"
	exit 1
fi

data=$1
bulk=$2
schema=$3

mkdir -p $bulk
cp dgraph.bulk.load.sh $bulk/
docker run --rm -it -v "$data:/data" -v "$bulk:/dgraph" dgraph/dgraph:v20.07.1 /dgraph/dgraph.bulk.load.sh "$schema" "${@:4}"
