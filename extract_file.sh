#!/bin/bash

set -euo pipefail

EXT=bz2

if [ $# -ne 1 ]
then
	echo "Please provide file to extract"
	exit 1
fi

file="$1"

echo "extracting $file"
bunzip2 -c "$file" > "${file/%.$EXT/}.inflate"
mv "${file/%.$EXT/}.inflate" "${file/%.$EXT/}"
