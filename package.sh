#!/bin/bash

set -euo pipefail

if [ $# -ne 1 ]
then
	echo "Please provide the path to the RDF files, e.g. dbpedia/2016-10/core-i18n"
	exit 1
fi

cd "$1"

echo -e "Generated with https://github.com/G-Research/dgraph-dbpedia/tree/master,\nderived from https://wiki.dbpedia.org/about and\nlicenced under https://creativecommons.org/licenses/by-sa/3.0/." > LICENCE.txt
for rdf in *.rdf
do
  echo -n "packaging $rdf"
  zip -r -0 "dgraph-dbpedia-v4.0-SNAPSHOT-$rdf.zip" LICENCE.txt "$rdf" | while read line; do echo -n .; done
  echo
done
