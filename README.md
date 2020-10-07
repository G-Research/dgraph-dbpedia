# Dgraph DBpedia Dataset

This projects prepares the [DBpedia dataset](http://downloads.dbpedia.org/)
for loading into [Dgraph](https://dgraph.io/). This comprises the steps
[download](#download-dbpedia), [extraction](#extract-dbpedia), [pre-processing](#pre-processing)
and [bulk loading](#run-dgraph-bulk-loader).
The first two steps can be done with provided shell scripts.
The third step by using [Apache Spark transformations](SPARK.md).
The last step uses the [Dgraph Bulk Loader](https://dgraph.io/docs/deploy/fast-data-loading/#bulk-loader).

## A large real-world dataset
I was looking for a large real-world graph dataset to load into a Dgraph cluster to ultimately test
my [spark-dgraph-connector](https://github.com/G-Research/spark-dgraph-connector).
Dgraph organizes the graph around predicates, so that dataset should contain predicates with these characteristics:

- numerous predicates, to have a **large real-world schema**
- a predicate that links a **deep hierarchy** of nodes
- a predicate that links a **deep network** of nodes
- a predicate that links **strongly connected components**
- a predicate with a lot of data, ideally a long string that exists for **every node**
- a predicate with geo coordinates (type Point)
- a **long-tail** predicate frequency distribution:
  a few predicates have high frequency (and low selectivity),
  most predicates have low frequency (and high selectivity)
- predicates that, if they exist for a node:
  - have a single occurrence (single value)
  - have a multiple occurrences (value list)
- real-world predicate names in multiple languages
- various data types and strings in multiple languages

A dataset that checks all these boxes can be found at the [DBpedia project](https://wiki.dbpedia.org/).
They extract structured information from the [Wikipedia project](https://wikipedia.org/) and provide them in RDF format.
However, that RDF data requires some preparation before it can be loaded into Dgraph.
Given the size of the datasets, a scalable pre-processing step is required.

This project uses [Apache Spark](https://spark.apache.org/) to bring real-work graph data into a Dgraph-compatible shape.
[Read the detailed tutorial on the pre-processing steps](SPARK.md).

## Requirements

This tutorial has the following requirements:

- Unix command line shell bash
- [Apache Maven](https://maven.apache.org/) installed
- [Docker](https://www.docker.com/) CLI installed
- Disk space: 12 GB for download, 100GB temporary space

## Datasets

This tutorial uses the following datasets from [DBpedia project](https://wiki.dbpedia.org/):

|dataset             |filename                        |description|
|--------------------|--------------------------------|-----------|
|labels              |`labels_{lang}.ttl`             |Each article has a single title in the article's language.|
|category            |`article_categories_{lang}.ttl` |Some articles link to categories, multiple categories allowed. Forms a category hierarchy.|
|inter-language links|`interlanguage_links_{lang}.ttl`|Articles link to the same article in all other languages. Forms strongly connected components.|
|page links          |`page_links_{lang}.ttl`         |Articles link to other articles or other resources. Forms a network of articles.|
|infobox             |`infobox_properties_{lang}.ttl` |Some articles have infoboxes. Provides structured information as key-value tables.|
|geo coordinates     |`geo_coordinates_{lang}.ttl`    |Some articles have geo coordinates of type `Point`.|

The `infobox` dataset provides real-world user-generated multi-language predicates.
The other datasets provide a fixed set of predicates each.

## Download DBpedia

Use the `download.sh` script to download the datasets and languages that you want to load into Dgraph:

    ./download.sh [path] [languages]

Both arguments `path` and `languages` are optional. Without, the script downloads all languages into
`./dbpedia`. To download only selected languages, run

    ./download.sh dbpedia "en es fr de"

You can find all available releases and datasets at http://downloads.dbpedia.org.
Stats for each release date are published in the `statsitics` sub-directory,
e.g. http://downloads.dbpedia.org/2016-10/statistics.

Downloading the four datasets in all languages will require 7 GB disk space.

## Extract DBpedia

DBpedia datasets are compressed and will be pre-processed using Spark. The compressed
files cannot be processed efficiently, so they have to be extracted first.

Run the `extract.sh` script:

    ./extract.sh dbpedia/2016-10

Extracting the four datasets in all languages will require 130 GB disk space.

## Pre-Processing

The provided Scala Spark code pre-processes the downloaded and extracted datasets
and produces [Dgraph compatible RDF triples](https://dgraph.io/docs/mutations/triples).

First we produce parquet files from all `ttl` files. All languages will be stored
in one parquet directory per dataset, where languages can still be selected in later steps.

    mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass="dgraph.dbpedia.DbpediaToParquetSparkApp" -Dexec.args="dbpedia 2016-10"

Secondly, process these parquet files into RDF triple files:

    mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass="dgraph.dbpedia.DbpediaDgraphSparkApp" -Dexec.args="dbpedia 2016-10"

These commands can optionally be given a comma separated list of language codes: `-Dexec.args="dbpedia 2016-10 en,es,fr,de"`.
Without those language codes, all languages will be processed.

There are more options at the beginning of the `main` method in `DbpediaDgraphSparkApp.scala`:

    val externaliseUris = false
    val removeLanguageTags = false
    val topInfoboxPropertiesPerLang = None
    val printStats = true

With `externaliseUris = true` the application turns all URIs into blank nodes and produces a `external_ids.rdf` file
which provides the `<xid>` predicate for each blank node with the URI as a string value.
See [External IDs](https://dgraph.io/docs/mutations/external-ids/) for more information.

Language tags can be removed from any value with `removeLanguageTags = true`. The `@lang` directives
are then also removed from the schema files `schema.dgraph` and `schema.indexed.dgraph`.

Only the `100` largest infobox properties are provided in the RDF files with `topInfoboxPropertiesPerLang = Some(100)`.
This can be used to control the size of the schema while allowing to add rich predicates.

With `printStats = false` you can turn-off some stats, which will reduce the processing time of the app.

### Memory Requirements

The `DbpediaDgraphSparkApp` requires 1 GB per CPU core. You can set the memory available to the application
via the `MAVEN_OPTS` environment variable:

    MAVEN_OPTS=-Xmx8g mvn compile exec:java …

On termination, the application prints some information like the following line:

    memory spill: 51 GB  disk spill: 4 GB  peak mem per host: 874 MB

This provides an indication if more memory should be given to the application. A huge number for
`disk spill` indicates lag of memory per core.

## Generated Dataset Files

Above example

- downloads to `dbpedia/2016-10/core-i18n/{lang}/{dataset}_{lang}.ttl.bz2`
- extracts to `dbpedia/2016-10/core-i18n/{lang}/{dataset}_{lang}.ttl`
- loads into `dbpedia/2016-10/core-i18n/{dataset}.parquet`
- processes to `dbpedia/2016-10/core-i18n/{dataset}.rdf`.

Individual languages can be found in `dbpedia/2016-10/core-i18n/{dataset}.rdf/lang={language}`.

Besides the datasets `article_categories.rdf`, `infobox_properties.rdf`, `interlanguage_links.rdf`, `labels.rdf`,
you can find external ids (when `externaliseUris = true`) in `external_ids.rdf`,
the schema for all predicates with and without indices in `schema.dgraph` and `schema.indexed.dgraph`, respectively,
as well as dgraph types of all nodes in `types.dgraph`.

## Run Dgraph Bulk Loader

Load all datasets and all languages:

    ./dgraph.bulk.sh $(pwd)/dbpedia/2016-10/core-i18n $(pwd)/dbpedia/2016-10/bulk "/data/schema.indexed.dgraph/*/part-*.txt" "/data/*.rdf/*/part-*.txt.gz"

Load a single dataset and language:

    export lang=de; export dataset=labels.rdf; ./dgraph.bulk.sh $(pwd)/dbpedia/2016-10/core-i18n $(pwd)/dbpedia/2016-10/bulk "/data/schema.indexed.dgraph/lang=any/part-*.txt /data/schema.dgraph/lang=$lang/part-*.txt" "/data/$dataset/lang=$lang/part-*.txt.gz"

The full dataset requires 64 GB RAM.

Either use `schema.indexed.dgraph` with bulk loader to populate the indices during bulk loading,
or bulk load with `schema.dgraph` and mutate the schema to `schema.indexed.dgraph` afterwards.

## Exploring the Graph

Start the Dgraph cluster on your bulk-loaded data:

    ./dgraph.serve.sh $(pwd)/dbpedia/2016-10/bulk

Then open up Ratel UI:

    http://localhost:8000/?latest#

Connect to the cluster and then query in the Console.

### Example Queries

Query for the first 10 nodes and their `uid`, `xid`, label, category and inter-language links:

    {
      query(func: has(<xid>), first: 10) {
        uid
        xid
        <http://www.w3.org/2000/01/rdf-schema#label>@*
        <http://purl.org/dc/terms/subject> { uid }
        <http://www.w3.org/2002/07/owl#sameAs> {
          uid
          xid
          <http://www.w3.org/2000/01/rdf-schema#label>@*
          <http://purl.org/dc/terms/subject> { uid }
          }
      }
    }

Result:

    {
      "data": {
        "query": [
          {
            "uid": "0x1",
            "xid": "http://es.dbpedia.org/resource/Diego_Alonso_de_Entenza_Rocafull_Vera_de_Mendoza_Zúñiga_Fajardo_Guzmán_Alburquerque_Portocarrero_Guevara_y_Otazu",
            "http://www.w3.org/2000/01/rdf-schema#label@es": "Diego Alonso de Entenza Rocafull Vera de Mendoza Zúñiga Fajardo Guzmán Alburquerque Portocarrero Guevara y Otazu"
          },
          {
            "uid": "0x2",
            "xid": "http://es.dbpedia.org/resource/Diego_Alvarado",
            "http://www.w3.org/2000/01/rdf-schema#label@es": "Diego Alvarado",
            "http://purl.org/dc/terms/subject": [ … ],
            "http://www.w3.org/2002/07/owl#sameAs": [
              {
                "uid": "0x2",
                "xid": "http://es.dbpedia.org/resource/Diego_Alvarado",
                "http://www.w3.org/2000/01/rdf-schema#label@es": "Diego Alvarado",
                "http://purl.org/dc/terms/subject": [ … ]
              },
              {
                "uid": "0x68d887",
                "xid": "http://it.dbpedia.org/resource/Diego_Alvarado",
                "http://www.w3.org/2000/01/rdf-schema#label@it": "Diego Alvarado"
              }
            ]
          }
        ]
      },
    }

Query for the wikipedia article with external URI `<http://dbpedia.org/resource/Andorra_(disambiguation)>`
and all inter-language labels:

    {
      query(func: eq(<xid>, "http://dbpedia.org/resource/Andorra_(disambiguation)")) {
        <http://www.w3.org/2002/07/owl#sameAs> {
          <http://www.w3.org/2000/01/rdf-schema#label>@*
        }
      }
    }

Result:

    {
      "data": {
        "query": [
          {
            "http://www.w3.org/2002/07/owl#sameAs": [
              {"http://www.w3.org/2000/01/rdf-schema#label@de": "Andorra (Begriffsklärung)"},
              {"http://www.w3.org/2000/01/rdf-schema#label@es": "Andorra (desambiguación)"},
              {"http://www.w3.org/2000/01/rdf-schema#label@it": "Andorra (disambigua)"},
              {"http://www.w3.org/2000/01/rdf-schema#label@fr": "Andorre (homonymie)"},
              {"http://www.w3.org/2000/01/rdf-schema#label@en": "Andorra (disambiguation)"}
            ]
          }
        ]
      }
    }

## Statistics

The following language codes are available for the `2016-10` datasets in `core-i18n`:

    ar az be bg bn ca cs cy de el en eo es eu fr ga gl hi hr hu hy id it ja ko lv mk nl pl pt ro ru sk sl sr sv tr uk vi zh

The datasets are bz2 compressed and 6.9 GB in size.

They extract to `.ttl` files of 129 GB size.

### Dataset Statistics

|dataset|triples|nodes|predicates|schema|
|:------|------:|----:|---------:|------|
|labels |55,001,940|55,001,935|1|`Article --rdfs:label-> lang string`|
|infobox_properties|295,278,129|21,261,665|482,461|`Article --property-> literal or uri`|
|top-100 infobox_properties|192,677,736|19,551,172|4,000|`Article --property-> literal or uri`|
|interlanguage_links|437,284,461|36,810,756|1|`Article --owl:sameAs-> Article`|
|article_categories|90,057,060|29,557,857|1|`Article --dcterms:subject-> Category`|
|all    |877,621,590|61,840,283|482,464||

### Language Statistics

|lang|labels  |category  |interlang links|infobox |infobox top 100 |
|:---:|------:|---------:|--------------:|-------:|---------------:|
|ar  |871,405  |1,942,195   |9,955,863        |4,574,804 |2,727,047         |
|az  |143,422  |219,855    |3,830,324        |783,088  |510,857          |
|be  |217,992  |307,957    |4,536,174        |1,036,032 |763,998          |
|bg  |333,098  |491,931    |6,349,430        |1,199,635 |751,318          |
|bn  |214,617  |152,577    |2,211,232        |676,004  |357,148          |
|ca  |868,322  |999,079    |10,321,447       |5,555,600 |3,829,640         |
|cs  |593,897  |1,455,928   |8,441,058        |3,548,877 |2,356,619         |
|cy  |124,873  |189,369    |2,952,191        |5,867,747 |5,758,012         |
|de  |334,3471 |8,185,499   |21,136,721       |14,048,415|8,898,574         |
|el  |185,210  |314,211    |4,071,132        |497,520  |235,298          |
|en  |12,845,252|23,990,512  |44,122,705       |52,680,098|26,966,738        |
|eo  |393,119  |527,912    |6,794,082        |1,949,066 |1,416,087         |
|es  |2,906,977 |3,622,137   |18,937,020       |10,858,241|5,925,338         |
|eu  |333,949  |427,983    |7,811,358        |2,046,149 |1,703,815         |
|fr  |3,241,245 |6,549,308   |23,160,557       |16,052,506|9,190,531         |
|ga  |45,636   |64,936     |1,691,513        |82,344   |49,163           |
|gl  |184,059  |317,526    |4,213,421        |597,615  |314,287          |
|hi  |157,515  |208,076    |2,391,518        |483,805  |278,581          |
|hr  |204,330  |281,986    |4,144,786        |1,274,116 |805,129          |
|hu  |577,761  |1,114,368   |9,222,488        |3,844,734 |2,021,889         |
|hy  |519,477  |420,750    |5,054,682        |2,656,709 |2,171,465         |
|id  |660,719  |596,902    |7,694,340        |2,753,661 |1,543,213         |
|it  |1,949,794 |1,786,162   |19,191,998       |20,207,833|13,932,773        |
|ja  |1,663,028 |4,271,371   |13,275,064       |7,763,985 |2,530,605         |
|ko  |670,310  |1,420,036   |9,167,596        |2,381,529 |1,074,711         |
|lv  |168,190  |174,077    |3,268,108        |798,879  |413,172          |
|mk  |128,202  |184,338    |3,970,015        |641,009  |346,036          |
|nl  |2,554,610 |2,764,083   |20,397,900       |8,918,883 |7,058,397         |
|pl  |1,575,762 |3,244,389   |16,959,809       |11,769,485|7,345,068         |
|pt  |1,667,327 |2,373,020   |16,773,266       |7,273,995 |4,220,599         |
|ro  |865,444  |826,522    |9,395,768        |6,192,337 |4,450,825         |
|ru  |3,033,613 |3,526,953   |19,085,698       |15,382,287|8,985,787         |
|sk  |278,133  |392,427    |6,671,402        |2,370,562 |1,554,812         |
|sl  |217,345  |467,838    |4,491,275        |1,355,263 |859,228          |
|sr  |873,929  |636,686    |7,588,286        |2,073,765 |1,377,521         |
|sv  |5,858,202 |8,408,876   |28,291,521       |41,295,967|38,208,922        |
|tr  |521,200  |1,041,967   |8,073,998        |2,991,863 |1,857,031         |
|uk  |1,049,249 |1,751,135   |12,681,031       |7,691,426 |4,292,580         |
|vi  |1,340,313 |2,185,821   |14,335,191       |14,322,161|11,726,191        |
|zh  |1,620,943 |2,220,362   |14,622,493       |8,780,134 |3,868,731         |

The `DbpediaToParquetSparkApp` tool takes half an hour on 8 cores machine with 2GB JVM memory.

The `DbpediaDgraphSparkApp` tool takes one to two hours on the same machine to produce the RDF files.

With sufficient RAM, the `en` datasets (with external URIs, 334.1 M edges) bulk loads in 17 minutes on an 8 cores machine with 14 GB free RAM.
