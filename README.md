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
the [spark-dgraph-connector](https://github.com/G-Research/spark-dgraph-connector).
Dgraph organizes the graph around predicates, so that dataset should contain predicates with these characteristics:

- a predicate that links a **deep hierarchy** of nodes
- a predicate that links a **deep network** of nodes
- a predicate that links **strongly connected components**
- a predicate with a lot of data, ideally a long string that exists for **every** node and with **multiple** languages
- a predicate with geo coordinates
- numerous predicates, to have a **large** schema
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
The size of the datasets requires a scalable pre-processing step.

This project uses [Apache Spark](https://spark.apache.org/) to bring real-work graph data into a Dgraph-compatible shape.
[Read the detailed tutorial on the pre-processing steps](SPARK.md).

## Requirements

This tutorial has the following requirements:

- Unix command line shell bash
- [Apache Maven](https://maven.apache.org/) installed
- [Docker](https://www.docker.com/) CLI installed
- A multi-core machine with SSD disk
- Disk space: 19 GB to download, 374 GB to extract, 42 GB for parquet, 75 GB temporary space

## Datasets

This tutorial uses the following datasets from [DBpedia project](https://wiki.dbpedia.org/):

|dataset             |filename                        |description|
|--------------------|--------------------------------|-----------|
|labels              |`labels_{lang}.ttl`             |Each article has a single title in the article's language.|
|category            |`article_categories_{lang}.ttl` |Some articles link to categories, multiple categories allowed.|
|skos                |`skos_categories_{lang}.ttl`    |Categories link to broader categories, forming a hierarchy. Forms a category hierarchy.|
|inter-language links|`interlanguage_links_{lang}.ttl`|Articles link to the same article in all other languages. Forms strongly connected components.|
|page links          |`page_links_{lang}.ttl`         |Articles link to other articles or other resources. Forms a network of articles.|
|infobox             |`infobox_properties_{lang}.ttl` |Some articles have infoboxes. Provides structured information as key-value tables.|
|geo coordinates     |`geo_coordinates_{lang}.ttl`    |Some articles have geo coordinates of type `Point`.|
|en_uris             |`{dataset}_en_uris_{lang}.ttl`  |Non-English `labels`, `infobox` and `category` predicates for English articles. Provides multiple language strings and predicates for articles.|

The `infobox` dataset provides real-world user-generated multi-language predicates.
The other datasets provide a fixed set of predicates each.

The `{dataset}_en_uris_{lang}.ttl` dataset is special. For three datasets `labels`, `infobox` and `category`,
it provides non-English data for Englisch articles. All data from these datasets are stored in parquet
and RDF under the `en-{lang}` languages. For instance, when you run `DbpediaDgraphSparkApp` with
languages `en` and `de`, then you will also get the `{dataset}_en_uris_de.ttl` datasets from language `en-de` as well.
Without the `en` language, you will not use any of the `{dataset}_en_uris_{lang}.ttl` datasets.

## Mix-and-Match your dataset

You can easily prepare any subset of this dataset. Download only those datasets and languages that you are interested in.
Start with a small language to go through these steps once. Then download and prepare all languages that you want.

## Download DBpedia

Use the `download.sh` script to download the datasets and languages that you want to load into Dgraph:

    ./download.sh [path] [languages]

Both arguments `path` and `languages` are optional. Without, the script downloads all languages into
`./dbpedia`. To download only selected languages, run

    ./download.sh dbpedia "en es fr de"

You can find all available releases and datasets at http://downloads.dbpedia.org.
Stats for each release date are published in the `statsitics` sub-directory,
e.g. http://downloads.dbpedia.org/2016-10/statistics.

Downloading the four datasets in all languages will require 19 GB disk space.

## Extract DBpedia

DBpedia datasets are compressed and will be pre-processed using Spark. The compressed
files cannot be processed efficiently, so they have to be extracted first.

Run the `extract.sh` script:

    ./extract.sh dbpedia/2016-10

Extracting the four datasets in all languages will require 374 GB disk space.

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
    val topInfoboxPropertiesPerLang = Some(100)
    val printStats = true

With `externaliseUris = true` the application turns all URIs into blank nodes and produces a `external_ids.rdf` file
which provides the `<xid>` predicate for each blank node with the URI as a string value.
See [External IDs](https://dgraph.io/docs/mutations/external-ids/) for more information.

Language tags can be removed from any value with `removeLanguageTags = true`. The `@lang` directives
are then also removed from the schema files `schema.dgraph` and `schema.indexed.dgraph`.

Only the `100` largest infobox properties are provided in the RDF files with `topInfoboxPropertiesPerLang = Some(100)`.
This can be used to control the size of the schema while allowing to add rich predicates.
Use `None` to get all 1 mio predicates from all datasets and languages.

With `printStats = false` you can turn-off some stats, which will reduce the processing time of the application.

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

    af als am an ar arz ast az azb ba bar be bg bn bpy br bs bug
    ca ce ceb ckb cs cv cy da de el en eo es et eu fa fi fo fr fy
    ga gd gl gu he hi hr hsb ht hu hy ia id io is it ja jv ka kk
    kn ko ku ky la lb li lmo lt lv mg min mk ml mn mr mrj ms my
    mzn nah nap nds ne new nl nn no oc or os pa pl pms pnb pt qu
    ro ru sa sah scn sco sh si sk sl sq sr su sv sw ta te tg th
    tl tr tt uk ur uz vec vi vo wa war yi yo zh

The datasets are `.bz2` compressed and 19 GB in size. They extract to `.ttl` files of 374 GB size.

Those loaded into parquet consume 42 GB, processed into TTL RDF files occupy 25 GB `.gz` compressed
and 363 GB uncompressed.

### Dataset Statistics

|dataset|triples|nodes|predicates|.bz |.ttl|.parquet|.rdf|schema|
|:------|------:|----:|---------:|---:|---:|-------:|---:|:-----|
|labels|94.410.463|76.478.687|1|1 GB|12 GB|2 GB|1 GB|`Article --rdfs:label-> lang string`|
|article_categories|149.254.994|41.655.032|1|1 GB|22 GB|3 GB|2 GB|`Article --dcterms:subject-> Category`|
|skos_categories|47.495.725|8.447.863|4|0.3 GB|8 GB|0.7 GB|0.4 GB|`Category --skos-core:broader-> Category`|
|interlanguage_links|656.814.200|49.426.513|1|5 GB|92 GB|11 GB|5 GB|`Article --owl:sameAs-> Article`|
|page_links|1.042.567.811|76.392.179|1|7 GB|154 GB|17 GB|10 GB|`Article --dbpedia:wikiPageWikiLink-> Article`|
|geo_coordinates|1.825.817|1.825.817|1|0.05 GB|1 GB|0.1 GB|0.03 GB|`Article --georss:point-> geoJSON`|
|infobox_properties|596.338.417|29.871.174|1.050.938|4 GB|87 GB| | |`Article --property-> literal or uri`|
|top-100 infobox_properties|364.484.645|27.594.565|10.571| | |9 GB|3 GB|`Article --property-> literal or uri`|
|all|2.594.184.878|86.737.376|1.050.949|19 GB|374 GB|42 GB|25 GB| |

Loading the entire dataset into parquets takes 2 hours on an 8-core machine with SSD disk and 2 GB JVM memory.
This requires 10 to 30 GB of temporary disk space, depending on the dataset that your are loading.

Processing the dataset into RDF takes 2 1/2 hours on a 4-core machine with SSD disk and 12 GB JVM memory.


### Language Statistics

These datasets provide the following number of triples per language:

|lang|labels  |infobox           |interlanguage      |page links|categories        |skos           |geo            |top 100 infobox           |
|:---|-------:|-----------------:|------------------:|---------:|-----------------:|--------------:|--------------:|-------------------------:|
|af  |98,211   |859,657            |2,016,334            |1,025,708   |116,777            |52,619          |null           |333,510                    |
|als |44,545   |399,078            |1,015,912            |544,847    |68,086             |20,070          |1              |235,629                    |
|am  |26,297   |46,466             |603,932             |116,211    |19,312             |4,458           |250            |29,174                     |
|an  |83,363   |694,952            |1,924,999            |1,155,374   |102,074            |111,563         |37             |393,096                    |
|ar  |1,171,851 |7,796,593           |9,955,863            |9,470,455   |3,391,286           |1,439,590        |12,899          |3,521,751                   |
|arz |33,190   |95,353             |867,441             |235,009    |70,387             |29,526          |689            |32,628                     |
|ast |91,961   |656,774            |1,727,916            |1,686,826   |214,238            |32,431          |205            |420,074                    |
|az  |213,278  |1,297,610           |3,830,324            |1,519,074   |303,057            |314,167         |780            |661,221                    |
|azb |17,634   |111,505            |259,403             |111,743    |14,259             |7,163           |28             |83,396                     |
|ba  |51,141   |665,640            |1,438,458            |448,334    |85,064             |119,519         |48             |497,059                    |
|bar |51,732   |317,827            |1,115,610            |633,300    |67,390             |42,389          |null           |198,076                    |
|be  |288,471  |1,642,606           |4,536,174            |2,492,728   |431,619            |339,609         |1,182           |956,902                    |
|bg  |495,223  |2,161,520           |6,349,430            |5,613,997   |783,533            |355,287         |2,394           |1,084,399                   |
|bn  |252,634  |1,230,809           |2,211,232            |1,238,572   |221,092            |129,492         |2,824           |514,901                    |
|bpy |44,470   |482,340            |1,090,864            |206,059    |47,438             |35,047          |242            |438,265                    |
|br  |129,816  |303,992            |2,702,414            |1,349,937   |193,811            |113,300         |25             |237,711                    |
|bs  |212,985  |1,683,670           |3,258,271            |2,531,453   |255,729            |277,436         |486            |956,612                    |
|bug |28,322   |5,199              |498,142             |128,339    |28,293             |1,977           |null           |4,974                      |
|ca  |1,223,510 |9,405,741           |10,321,447           |15,628,064  |1,479,729           |362,873         |89,582          |5,613,442                   |
|ce  |165,351  |2,260,919           |1,177,542            |1,086,175   |167,638            |17,924          |748            |2,181,136                   |
|ceb |5,213,365 |53,241,289          |17,932,724           |40,602,693  |6,205,339           |568,490         |297,725         |52,265,849                  |
|ckb |72,116   |221,007            |1,767,422            |256,694    |86,148             |154,328         |710            |138,729                    |
|cs  |834,258  |5,958,462           |8,441,058            |13,186,669  |2,140,466           |690,819         |1              |3,126,986                   |
|cv  |52,848   |431,221            |1,069,020            |484,472    |74,412             |18,219          |57             |314,378                    |
|cy  |184,514  |9,623,193           |2,952,191            |4,791,110   |297,470            |178,252         |1,114           |9,332,825                   |
|da  |502,972  |3,025,206           |6,149,207            |6,026,849   |874,407            |330,542         |18,871          |1,158,124                   |
|de  |4,361,792 |20,764,210          |21,136,721           |68,802,723  |11,060,348          |1,532,872        |9              |10,579,783                  |
|el  |274,062  |905,261            |4,071,132            |3,447,861   |513,208            |262,866         |12,774          |325,169                    |
|en  |12,845,252|52,680,098          |44,122,705           |183,605,695 |23,990,512          |6,083,029        |580,892         |26,966,738                  |
|eo  |568,314  |3,456,160           |6,794,082            |6,008,777   |845,865            |341,505         |null           |1,871,133                   |
|es  |3,719,308 |18,440,582          |18,937,020           |44,035,493  |5,425,875           |1,987,929        |40,737          |7,624,024                   |
|et  |350,187  |850,774            |4,196,133            |4,392,950   |470,466            |155,112         |507            |471,801                    |
|eu  |537,261  |3,864,551           |7,811,358            |4,192,367   |712,793            |329,052         |null           |3,039,470                   |
|fa  |2,300,470 |12,555,126          |12,366,593           |10,296,006  |4,380,211           |3,090,009        |31,687          |5,887,203                   |
|fi  |920,303  |6,632,480           |8,498,945            |10,472,218  |1,674,538           |411,208         |16,105          |2,805,052                   |
|fo  |27,861   |89,958             |1,058,896            |235,030    |39,073             |27,334          |38             |40,935                     |
|fr  |4,405,822 |26,447,655          |23,160,557           |64,055,280  |9,265,549           |2,012,049        |37,801          |12,133,693                  |
|fy  |79,309   |164,638            |1,534,960            |1,087,488   |158,606            |121,377         |null           |109,643                    |
|ga  |81,378   |148,219            |1,691,513            |475,562    |106,108            |31,077          |680            |67,616                     |
|gd  |35,310   |39,047             |803,105             |229,342    |38,800             |7,837           |1,133           |32,585                     |
|gl  |266,483  |1,055,460           |4,213,421            |4,199,110   |490,729            |265,139         |10,860          |412,570                    |
|gu  |35,737   |453,364            |447,122             |557,348    |69,274             |5,243           |190            |386,950                    |
|he  |510,377  |2,314,229           |5,474,382            |9,061,515   |985,486            |305,831         |15,205          |1,066,260                   |
|hi  |207,686  |746,882            |2,391,518            |1,612,841   |264,268            |120,362         |3              |317,595                    |
|hr  |311,229  |2,123,866           |4,144,786            |4,086,321   |447,005            |99,734          |3,137           |1,007,664                   |
|hsb |23,075   |167,028            |736,306             |191,381    |31,116             |23,670          |6              |140,398                    |
|ht  |89,435   |383,066            |1,157,907            |360,968    |170,140            |6,483           |6              |373,604                    |
|hu  |864,527  |6,656,561           |9,222,488            |12,321,943  |1,702,625           |317,175         |null           |2,489,386                   |
|hy  |623,673  |4,051,530           |5,054,682            |3,911,694   |543,949            |233,449         |1,141           |2,721,660                   |
|ia  |39,132   |2,333              |995,279             |135,455    |31,918             |18,859          |11             |1,688                      |
|id  |854,181  |4,285,674           |7,694,340            |8,288,264   |848,986            |321,023         |12,455          |1,806,378                   |
|io  |57,289   |187,666            |1,588,619            |443,393    |61,624             |37,101          |3              |157,132                    |
|is  |93,287   |267,951            |1,998,978            |876,343    |90,553             |78,062          |61             |158,986                    |
|it  |2,833,716 |33,123,972          |19,191,998           |43,346,247  |2,666,216           |1,607,375        |15,681          |18,180,164                  |
|ja  |2,122,644 |12,097,187          |13,275,064           |51,368,506  |5,782,533           |1,164,828        |25,338          |2,918,038                   |
|jv  |86,530   |616,346            |1,751,634            |834,417    |78,443             |73,217          |532            |372,939                    |
|ka  |222,991  |2,157,526           |3,949,501            |1,994,541   |337,788            |195,979         |2,433           |1,235,656                   |
|kk  |360,611  |5,277,512           |4,649,156            |2,260,768   |414,994            |106,775         |605            |4,683,721                   |
|kn  |38,281   |262,247            |649,336             |549,420    |53,831             |11,541          |274            |107,419                    |
|ko  |901,166  |3,840,849           |9,167,596            |9,968,592   |2,239,199           |1,329,189        |16,353          |1,281,614                   |
|ku  |46,658   |120,041            |973,497             |311,644    |46,931             |32,273          |null           |82,175                     |
|ky  |85,540   |858,653            |1,196,064            |337,477    |95,842             |7,015           |19             |816,394                    |
|la  |292,107  |731,409            |4,886,226            |2,491,031   |487,031            |203,950         |710            |502,918                    |
|lb  |89,700   |269,889            |2,160,453            |1,322,664   |158,379            |137,052         |125            |197,672                    |
|li  |52,536   |19,668             |616,604             |367,905    |26,206             |10,675          |1              |17,742                     |
|lmo |79,838   |891,581            |1,472,013            |459,111    |33,757             |19,661          |358            |662,629                    |
|lt  |360,399  |2,511,128           |4,269,322            |4,031,031   |402,476            |141,959         |7,581           |1,470,356                   |
|lv  |220,610  |1,351,505           |3,268,108            |1,918,216   |281,367            |159,485         |1,109           |520,503                    |
|mg  |197,167  |624,830            |2,003,552            |954,285    |261,720            |10,768          |34,897          |620,180                    |
|min |246,389  |1,959,891           |1,792,798            |1,753,645   |237,842            |4,446           |41             |1,916,033                   |
|mk  |186,205  |1,075,375           |3,970,015            |2,095,968   |284,779            |364,506         |4,304           |421,342                    |
|ml  |150,603  |810,329            |1,698,480            |931,001    |138,271            |93,442          |1,213           |317,520                    |
|mn  |38,023   |321,824            |1,126,634            |364,438    |70,109             |56,395          |93             |121,216                    |
|mr  |113,663  |625,068            |1,832,585            |639,334    |131,620            |68,690          |579            |208,877                    |
|mrj |21,845   |977               |530,065             |122,714    |28,993             |6,696           |null           |892                       |
|ms  |533,602  |6,016,867           |6,096,651            |4,479,346   |617,401            |145,199         |3,757           |3,661,592                   |
|my  |50,186   |526,621            |797,111             |354,978    |94,075             |19,999          |139            |429,080                    |
|mzn |27,713   |177,861            |424,453             |113,356    |22,627             |6,467           |275            |129,001                    |
|nah |25,629   |94,021             |684,279             |102,130    |17,208             |12,423          |9              |48,590                     |
|nap |29,055   |26,731             |845,177             |185,862    |61,728             |6,467           |24             |12,254                     |
|nds |54,096   |165,908            |1,092,700            |739,428    |81,887             |13,834          |null           |131,264                    |
|ne  |52,430   |385,551            |862,994             |294,915    |88,843             |8,307           |267            |210,777                    |
|new |122,826  |439,710            |1,397,549            |811,948    |80,699             |110,973         |40             |389,517                    |
|nl  |3,335,991 |14,045,600          |20,397,900           |31,823,749  |3,928,588           |648,733         |273            |9,573,653                   |
|nn  |311,357  |1,976,905           |4,112,662            |2,787,235   |585,683            |373,834         |154            |1,127,559                   |
|no  |1,017,384 |4,939,725           |9,187,809            |12,087,904  |2,479,938           |784,693         |1              |2,223,737                   |
|oc  |176,482  |2,905,476           |3,662,562            |1,809,710   |182,003            |91,343          |290            |2,780,756                   |
|or  |31,536   |215,056            |783,907             |155,077    |38,966             |21,633          |129            |114,128                    |
|os  |27,437   |146,036            |890,260             |103,564    |21,447             |32,159          |57             |108,284                    |
|pa  |53,726   |459,389            |955,538             |312,123    |40,902             |14,003          |1              |213,601                    |
|pl  |2,327,607 |19,687,168          |16,959,809           |30,792,002  |4,748,848           |928,082         |null           |9,930,861                   |
|pms |124,141  |1,259,280           |1,973,337            |559,036    |131,833            |24,502          |1,684           |1,205,568                   |
|pnb |79,567   |148,372            |1,481,796            |561,884    |115,983            |18,466          |30             |102,777                    |
|pt  |2,327,744 |12,788,849          |16,773,266           |26,111,747  |3,788,231           |1,460,842        |7,566           |5,991,711                   |
|qu  |55,280   |108,873            |1,230,374            |482,813    |78,704             |36,600          |6              |62,440                     |
|ro  |1,113,731 |9,782,477           |9,395,768            |8,715,289   |1,277,714           |838,390         |8,412           |5,724,342                   |
|ru  |3,742,207 |24,364,949          |19,085,698           |42,153,229  |5,124,814           |1,834,910        |17,756          |10,844,135                  |
|sa  |29,628   |98,492             |961,493             |153,809    |68,943             |34,479          |69             |49,532                     |
|sah |22,832   |82,958             |760,517             |134,674    |25,044             |22,249          |69             |36,521                     |
|scn |62,420   |50,544             |1,238,920            |317,603    |52,774             |8,045           |2              |39,576                     |
|sco |93,318   |1,295,368           |2,500,416            |1,023,709   |336,061            |206,390         |4,470           |592,022                    |
|sh  |4,170,852 |7,383,379           |7,642,414            |11,431,459  |1,118,999           |270,882         |2,143           |4,449,314                   |
|si  |32,398   |161,156            |515,678             |188,502    |20,525             |15,373          |556            |57,032                     |
|sk  |428,246  |4,250,370           |6,671,402            |4,528,315   |618,394            |399,109         |104            |2,362,093                   |
|sl  |316,293  |2,452,078           |4,491,275            |4,883,529   |704,342            |299,890         |null           |1,250,299                   |
|sq  |124,285  |690,644            |2,153,865            |980,879    |170,617            |51,483          |2,985           |415,314                    |
|sr  |1,104,870 |3,609,351           |7,588,286            |7,183,655   |1,009,661           |223,429         |1,450           |1,869,583                   |
|su  |31,952   |141,434            |890,365             |322,313    |41,145             |29,672          |280            |81,555                     |
|sv  |6,564,694 |47,913,503          |28,291,521           |63,821,423  |9,914,119           |1,605,573        |299,191         |40,544,064                  |
|sw  |78,374   |275,543            |1,748,641            |591,589    |102,908            |63,680          |214            |198,052                    |
|ta  |183,325  |1,372,455           |2,185,580            |1,487,866   |275,041            |92,031          |2,703           |569,867                    |
|te  |107,472  |1,292,083           |957,048             |1,140,325   |119,931            |39,578          |548            |1,023,050                   |
|tg  |113,021  |621,239            |1,308,004            |532,675    |142,637            |46,149          |6,517           |547,269                    |
|th  |321,911  |2,182,301           |3,909,870            |3,246,722   |463,829            |283,922         |3,846           |686,830                    |
|tl  |216,811  |429,697            |2,355,004            |772,135    |135,541            |64,834          |969            |200,911                    |
|tr  |714,017  |4,915,611           |8,073,998            |7,850,843   |1,693,920           |1,152,553        |8,897           |2,307,816                   |
|tt  |139,474  |784,338            |2,000,726            |937,155    |135,914            |87,777          |869            |623,940                    |
|uk  |1,421,957 |12,556,757          |12,681,031           |16,777,605  |2,565,040           |813,702         |55,597          |5,628,647                   |
|ur  |391,040  |3,800,171           |5,107,065            |1,802,815   |406,616            |1,573,935        |1,326           |2,437,277                   |
|uz  |532,111  |4,632,138           |3,828,422            |1,061,031   |163,293            |60,910          |45             |4,453,082                   |
|vec |27,216   |219,389            |855,523             |174,803    |19,205             |15,949          |43             |163,829                    |
|vi  |1,767,412 |20,343,876          |14,335,191           |15,831,995  |2,937,313           |928,497         |5,895           |14,005,396                  |
|vo  |359,259  |1,066,887           |3,268,237            |971,438    |585,644            |6,967           |237            |1,062,062                   |
|wa  |33,578   |265               |405,923             |203,900    |13,244             |7,142           |1              |265                       |
|war |2,365,132 |14,592,781          |13,360,276           |16,711,018  |2,241,904           |508,050         |49             |14,557,309                  |
|yi  |33,677   |46,023             |701,582             |232,315    |32,388             |12,289          |908            |31,851                     |
|yo  |70,494   |213,481            |1,364,255            |194,529    |59,524             |18,219          |192            |94,591                     |
|zh  |2,118,647 |14,301,875          |14,622,493           |23,162,842  |3,055,350           |1,342,714        |76,187          |4,748,106                   |
