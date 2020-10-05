#!/bin/bash

set -euo pipefail

EXT=bz2

if [ $# -ne 1 ]
then
	echo "Please provide the path to the release, e.g. 2016-10"
	exit 1
fi

base="$1"

echo "Extracting these files:"
find -L "$base" -name "*.$EXT" | while read file; do echo $file; if [ ! -e "${file/%.$EXT/}" ]; then echo $file; fi; done | sort
echo

find -L "$base" -name "*.$EXT" | while read file; do if [ ! -e "${file/%.$EXT/}" ]; then echo $file; fi; done | sort | while read file
do
	echo "extracting $file"
	bunzip2 -c "$file" > "${file/%.bz2/}.inflate"
	mv "${file/%.bz2/}.inflate" "${file/%.bz2/}"
done

