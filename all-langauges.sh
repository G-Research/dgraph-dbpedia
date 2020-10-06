#!/bin/bash

RELEASE=2016-10
DATASET=core-i18n

echo "All languages for $RELEASE release of $DATASET dataset:"
echo $(curl -s "http://downloads.dbpedia.org/$RELEASE/$DATASET/" | grep -E -e 'href="[a-z]{2,3}\/?"' | sed -E -e 's/.*href="([^"]+)\/".*/\1/')

