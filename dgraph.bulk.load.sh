#!/bin/bash

schema_file=/dgraph/schema.dgraph
# data dir will be truncated
data_dir=/dgraph/data

# cat all schema files to a single file
schema=$1
cat $schema > "$schema_file"
shift

# link all data files from data dir
mkdir -p "$data_dir" 
rm -f "$data_dir"/*
for file in $(ls -d ${@})
do
  ln -s "$file" "$data_dir/$(echo "$file" | md5sum | sed -e 's/ .*//')"
done

echo "bulk loading schema $schema"
echo "bulk loading files: ${@}"
echo

function join { local IFS="$1"; shift; echo "$*"; }

# start zero
echo "starting zero"
rm -rf /dgraph/zw
dgraph zero >> /data/zero.log 2>&1 < /dev/null &
sleep 5

# start bulk loader
echo "bulk loading"
rm -rf /dgraph/out /dgraph/xidmap
dgraph bulk --store_xids --xidmap /dgraph/xidmap --mapoutput_mb 10 -j 4 --ignore_errors --tmp /dgraph/tmp -f "$(join , $(ls $data_dir/*))" -s "$schema_file" --format=rdf --out=/dgraph/out --replace_out 2>&1 | tee /dgraph/bulk.log
