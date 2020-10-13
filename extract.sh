#!/bin/bash

set -euo pipefail

EXT=bz2

if [ $# -ne 1 ]
then
	echo "Please provide the path to the release, e.g. dbpedia/2016-10"
	exit 1
fi

base="$1"
extract=$(dirname "$0")

# extract files in parallel and in reverse file size order
#find -L "$base" -name "*.$EXT" -exec ls -s "{}" ";" | sort -nr | cut -d " " -f 2- | while read file; do if [ ! -e "${file/%.$EXT/}" ]; then echo $file; fi; done | sort | parallel --will-cite --progress --eta $extract/extract_file.sh

find -L "$base" -name "*.$EXT" | while read file; do if [ ! -e "${file/%.$EXT/}" ]; then echo $file; fi; done | sort | while read file
do
	$extract/extract_file.sh "$file"
done
