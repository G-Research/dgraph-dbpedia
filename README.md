# Dgraph DBpedia Dataset

This projects prepares the [DBpedia dataset](http://downloads.dbpedia.org/)
for loading into [Dgraph](https://dgraph.io/). This comprises the steps
[download](#download-dbpedia), [extraction](#extract-dbpedia) and [pre-processing](#pre-processing).
The first two steps can be done with provided shell scripts.
The last step is done using [Apache Spark](https://spark.apache.org) transformations.

## Download DBpedia

Use the `download.sh` script to download the datasets and languages that you want to load into Dgraph:

    sh download.sh

Configure the first block of variables in that file if to your needs:

    RELEASE=2016-10
    DATASET=core-i18n
    LANGS="en fr de ru es nl it pt pl ja zh"
    FILENAMES="labels infobox_properties interlanguage_links article_categories"
    EXT=.ttl.bz2

You can find all available releases and datasets at http://downloads.dbpedia.org.
Stats for each release date are published in the `statsitics` sub-directory,
e.g. http://downloads.dbpedia.org/2016-10/statistics.

## Extract DBpedia

DBpedia datasets are compressed and will be pre-processed using Spark. The compressed
files cannot be processed efficiently, so they have to be extracted first.

Run the `extract.sh` script:

    sh extract.sh 2016-10

## Pre-Processing

The provided Scala Spark code preprocesses the downloaded and extracted datasets
and produces [Dgraph compatible RDF triples](https://dgraph.io/docs/mutations/triples).

## Produce RDF files

Concat or load individual small files.


## stats

all langs
ar,az,be,bg,bn,ca,cs,cy,de,el,en,eo,es,eu,fr,ga,gl,hi,hr,hu,hy,id,it,ja,ko,lv,mk,nl,pl,pt,ro,ru,sk,sl,sr,sv,tr,uk,vi,zh

size compressed
6.9 GB bz2

size extracted
129 GB ttl

stats
labels: 55,001,940 triples, 55,001,935 nodes, 1 predicates
infobox_properties: 295,278,129 triples, 21,261,665 nodes, 482,461 predicates
interlanguage_links: 437,284,461 triples, 36,810,756 nodes, 1 predicates
article_categories: 90,057,060 triples, 29,557,857 nodes, 1 predicate
all: 877,621,590 triples, 61,840,283 nodes, 482,464 predicates

schema
labels: Article --rdfs:label-> lang string
infobox_properties: Article --property-> literal or uri
interlanguage_links: Article --owl:sameAs-> Article
article_categories: Article --dcterms:subject-> Category

spark setup
8 cores, 2GB jvm RAM

time to parquet
finished in 0h 33m 59s

time to rdf
URIs will be externalized
cleaned up infoboxes cover 88% of original rows
finished in 1h 24m 35s
