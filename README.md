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

Run the following command on an Ubuntu instance to get it setup:

    sudo apt-get update
    sudo apt-get install -y git maven docker
    sudo usermod -aG docker ${USER}
    git clone https://github.com/EnricoMi/dgraph-dbpedia.git
    cd dgraph-dbpedia

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
it provides non-English data for English articles. All data from these datasets are stored in parquet
and RDF under the `en-{lang}` languages. For instance, when you run `DbpediaDgraphSparkApp` with
languages `en` and `de`, then you will also get the `{dataset}_en_uris_de.ttl` dataset as language `en-de` as well.
Without the `en` language, you will not get any of the `{dataset}_en_uris_{lang}.ttl` datasets.

## Mix-and-Match your dataset

You can easily prepare any subset of this dataset. Download only those datasets and languages that you are interested in.
Start with a small language to go through these steps once. Then download and prepare all languages that you want.

Even when you download and pre-process all datasets and all languages into RDF you will be able
to pick datasets and languages when bulk-loading data into Dgraph. RDF files are split by languages
and the schema is additionally split by dataset, so that you can easily load the schema only of the dataset
that you are bulk loading.

## Download DBpedia

Use the `download.sh` script to download the datasets and languages that you want to load into Dgraph:

    ./download.sh [path] [languages]

Both arguments `path` and `languages` are optional. Without, the script downloads all languages into
`./dbpedia`. To download only selected languages, run

    ./download.sh dbpedia "en es fr zh jp"

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

    MAVEN_OPTS=-Xmx2g mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass="dgraph.dbpedia.DbpediaToParquetSparkApp" -Dexec.args="dbpedia 2016-10"

Secondly, process these parquet files into RDF triple files:

    MAVEN_OPTS=-Xmx8g mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass="dgraph.dbpedia.DbpediaDgraphSparkApp" -Dexec.args="dbpedia 2016-10"

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

The `DbpediaDgraphSparkApp` requires at least 1 GB per CPU core, ideally 2 GB.
You can set the memory available to the application via the `MAVEN_OPTS` environment variable:

    MAVEN_OPTS=-Xmx8g mvn compile exec:java …

On termination, the application prints some information like the following line:

    memory spill: 51 GB  disk spill: 4 GB  peak mem per host: 874 MB

This provides an indication if more memory should be given to the application.
A huge number (upper two digits GB) for `disk spill` indicates lag of memory per core.

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

Load a subset of datasets and languages, defined via `langs` and `datasets`:

    export langs="en|en-es|en-fr|en-zh|en-jp"; export datasets="labels|infobox_properties"; ./dgraph.bulk.sh $(pwd)/dbpedia/2016-10/core-i18n $(pwd)/dbpedia/2016-10/bulk "/data/schema.dgraph/dataset=@($datasets)/lang=@($langs|any)/part-*.txt" "/data/@($datasets).rdf/lang=@($langs)/part-*.txt.gz"

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

Query for the wikipedia article about [Wikipedia](https://en.wikipedia.org/wiki/Wikipedia) (http://dbpedia.org/page/Wikipedia):

    {
      query(func: eq(<xid>, "http://dbpedia.org/resource/Wikipedia")) {
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
            "uid": "0x350f7c",
            "xid": "http://dbpedia.org/resource/Wikipedia",
            "http://www.w3.org/2000/01/rdf-schema#label@en": "Wikipedia",
            "http://www.w3.org/2000/01/rdf-schema#label@zh": "维基百科",
            "http://www.w3.org/2000/01/rdf-schema#label@es": "Wikipedia",
            "http://www.w3.org/2000/01/rdf-schema#label@fr": "Wikipédia"
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
and 317 GB uncompressed.

### Dataset Statistics

|dataset|triples|nodes|predicates|.bz |.ttl|.parquet|.rdf|schema|
|:------|------:|----:|---------:|---:|---:|-------:|---:|:-----|
|labels|94.410.463|76.478.687|1|1 GB|12 GB|2 GB|1 GB|`Article --rdfs:label-> lang string`|
|article_categories|149.254.994|41.655.032|1|1 GB|22 GB|3 GB|2 GB|`Article --dcterms:subject-> Category`|
|skos_categories|47.495.725|8.447.863|4|0.3 GB|8 GB|0.7 GB|0.4 GB|`Category --skos-core:broader-> Category`|
|interlanguage_links|546.769.314| |1|5 GB|92 GB|11 GB|5 GB|`Article --owl:sameAs-> Article`|
|page_links|1.042.567.811|76.392.179|1|7 GB|154 GB|17 GB|10 GB|`Article --dbpedia:wikiPageWikiLink-> Article`|
|geo_coordinates|1.825.817|1.825.817|1|0.05 GB|1 GB|0.1 GB|0.03 GB|`Article --georss:point-> geoJSON`|
|infobox_properties|596.338.417|29.871.174|1.050.938|4 GB|87 GB| | |`Article --property-> literal or uri`|
|top-100 infobox_properties|298.289.529| |10.571| | |9 GB|3 GB|`Article --property-> literal or uri`|
|all| | not sum |1.050.949|19 GB|374 GB|42 GB|25 GB| |

Loading the entire dataset into parquets takes 2 hours on an 8-core machine with SSD disk and 2 GB JVM memory.
This requires 10 to 30 GB of temporary disk space, depending on the dataset that your are loading.

Processing the dataset into RDF takes 1 1/2 hours on a 4-core machine with SSD disk and 12 GB JVM memory.


### Language Statistics

These datasets provide the following number of triples per language:

<!--
paste the table printed by `DbpediaDgraphSparkApp` when run on the full dataset here
then replace (\d)(\d{3}\D) with $1,$2 until no matches exist anymore (adds thousand separator character)
then remove all null
-->

|lang|labels  |interlanguage      |page links|categories        |skos           |geo            |infobox           |top 100 infobox           |
|:---|-------:|------------------:|---------:|-----------------:|--------------:|--------------:|-----------------:|-------------------------:|
|af  |62902   |1875080            |1025708   |64901             |27722          |               |468032            |254962                    |
|als |27077   |942457             |544847    |45067             |12328          |1              |216224            |154004                    |
|am  |19697   |566489             |116211    |16300             |3194           |250            |28372             |23039                     |
|an  |57240   |1790964            |1155374   |62983             |65608          |37             |384921            |267495                    |
|ar  |871405  |8456373            |9470455   |1942195           |797717         |12899          |4574804           |2727047                   |
|arz |22803   |802974             |235009    |48724             |19255          |689            |54399             |26510                     |
|ast |55036   |1588057            |1686826   |161813            |20438          |205            |396939            |299062                    |
|az  |143422  |3436415            |1519074   |219855            |194832         |780            |783088            |510857                    |
|azb |13391   |236252             |111743    |13468             |6828           |28             |76616             |65412                     |
|ba  |42116   |1274257            |448334    |71464             |76453          |48             |500493            |431324                    |
|bar |34916   |1038524            |633300    |48230             |27264          |               |189491            |144376                    |
|be  |217992  |4071053            |2492728   |307957            |216374         |1182           |1036032           |763998                    |
|bg  |333098  |5662926            |5613997   |491931            |209189         |2394           |1199635           |751318                    |
|bn  |214617  |2017159            |1238572   |152577            |86507          |2824           |676004            |357148                    |
|bpy |25404   |1013214            |206059    |33187             |18421          |242            |288330            |278184                    |
|br  |79421   |2486979            |1349937   |122690            |66130          |25             |163068            |142927                    |
|bs  |170881  |2963799            |2531453   |184450            |189966         |486            |1122106           |846358                    |
|bug |14230   |459340             |128339    |14239             |1158           |               |3345              |3286                      |
|ca  |868322  |8919143            |15628064  |999079            |254533         |89582          |5555600           |3829640                   |
|ce  |157642  |870894             |1086175   |165082            |14290          |748            |2152346           |2126202                   |
|ceb |4864101 |12223691           |40602693  |6060892           |543714         |297725         |48599129          |48590356                  |
|ckb |56402   |1623083            |256694    |46733             |78501          |710            |121163            |88801                     |
|cs  |593897  |7280033            |13186669  |1455928           |441698         |1              |3548877           |2356619                   |
|cv  |38674   |972803             |484472    |66606             |12986          |57             |322440            |277126                    |
|cy  |124873  |2670515            |4791110   |189369            |99216          |1114           |5867747           |5758012                   |
|da  |353730  |5433263            |6026849   |574511            |191523         |18871          |1827254           |967812                    |
|de  |3343471 |16120815           |68802723  |8185499           |1155538        |9              |14048415          |8898574                   |
|el  |185210  |3663190            |3447861   |314211            |153419         |12774          |497520            |235298                    |
|en  |12845252|29476295           |183605695 |23990512          |6083029        |580892         |52680098          |26966738                  |
|en-*|17933697|                   |          |36147553          |14548093       |               |178020543         |24657904                  |
|eo  |393119  |6096191            |6008777   |527912            |203701         |               |1949066           |1416087                   |
|es  |2906977 |15451078           |44035493  |3622137           |1348394        |40737          |10858241          |5925338                   |
|et  |260421  |3761071            |4392950   |320249            |92385          |507            |501304            |339731                    |
|eu  |333949  |7018090            |4192367   |427983            |202547         |               |2046149           |1703815                   |
|fa  |1891729 |10313741           |10296006  |2381913           |1652569        |31687          |6827706           |4054788                   |
|fi  |637527  |7364185            |10472218  |1034362           |240662         |16105          |3793465           |2166251                   |
|fo  |17243   |1000468            |235030    |22348             |15002          |38             |49932             |30778                     |
|fr  |3241245 |18386234           |64055280  |6549308           |1394960        |37801          |16052506          |9190531                   |
|fy  |56325   |1397933            |1087488   |104970            |74218          |               |92718             |74624                     |
|ga  |45636   |1574022            |475562    |64936             |19600          |680            |82344             |49163                     |
|gd  |22609   |758946             |229342    |26292             |5415           |1133           |22202             |20921                     |
|gl  |184059  |3767468            |4199110   |317526            |175457         |10860          |597615            |314287                    |
|gu  |29004   |381512             |557348    |59909             |3521           |190            |406231            |375655                    |
|he  |360725  |4848271            |9061515   |614735            |185868         |15205          |1287035           |757865                    |
|hi  |157515  |2088013            |1612841   |208076            |75325          |3              |483805            |278581                    |
|hr  |204330  |3720040            |4086321   |281986            |61525          |3137           |1274116           |805129                    |
|hsb |15246   |684769             |191381    |21852             |14075          |6              |100225            |93021                     |
|ht  |57587   |1033648            |360968    |126744            |3804           |6              |245047            |243681                    |
|hu  |577761  |8135897            |12321943  |1114368           |202481         |               |3844734           |2021889                   |
|hy  |519477  |4474158            |3911694   |420750            |152196         |1141           |2656709           |2171465                   |
|ia  |23041   |939137             |135455    |17079             |10803          |11             |1334              |1139                      |
|id  |660719  |6611139            |8288264   |596902            |212629         |12455          |2753661           |1543213                   |
|io  |30601   |1492156            |443393    |37164             |20514          |3              |96793             |89943                     |
|is  |65226   |1848537            |876343    |61213             |46129          |61             |171318            |128153                    |
|it  |1949794 |15542736           |43346247  |1786162           |1161845        |15681          |20207833          |13932773                  |
|ja  |1663028 |10522965           |51368506  |4271371           |768230         |25338          |7763985           |2530605                   |
|jv  |64481   |1580360            |834417    |61553             |54885          |532            |436879            |320303                    |
|ka  |144916  |3587693            |1994541   |208311            |114438         |2433           |1236797           |897810                    |
|kk  |267062  |4107418            |2260768   |326357            |68748          |605            |2998438           |2836655                   |
|kn  |27449   |590437             |549420    |47789             |9503           |274            |169705            |94659                     |
|ko  |670310  |7838730            |9968592   |1420036           |777602         |16353          |2381529           |1074711                   |
|ku  |36155   |893677             |311644    |34256             |20105          |               |78004             |64317                     |
|ky  |61403   |1071799            |337477    |86725             |5528           |19             |452740            |436223                    |
|la  |176958  |4474648            |2491031   |286766            |131377         |710            |380644            |307013                    |
|lb  |58701   |1982488            |1322664   |105622            |86392          |125            |172366            |145641                    |
|li  |43769   |578532             |367905    |17166             |7151           |1              |10789             |10527                     |
|lmo |48949   |1369828            |459111    |24824             |15727          |358            |466870            |430165                    |
|lt  |259153  |3766343            |4031031   |277495            |90806          |7581           |1526071           |1093156                   |
|lv  |168190  |2991527            |1918216   |174077            |95940          |1109           |798879            |413172                    |
|mg  |124431  |1797354            |954285    |186863            |8580           |34897          |337922            |336717                    |
|min |222999  |1341378            |1753645   |219550            |3496           |41             |1876798           |1861140                   |
|mk  |128202  |3598089            |2095968   |184338            |217204         |4304           |641009            |346036                    |
|ml  |117149  |1528680            |931001    |97310             |66791          |1213           |488500            |260402                    |
|mn  |24143   |1048103            |364438    |49038             |35692          |93             |179136            |88706                     |
|mr  |82601   |1682500            |639334    |95619             |47193          |579            |350277            |165420                    |
|mrj |12491   |498892             |122714    |16304             |3707           |               |568               |562                       |
|ms  |332229  |5382317            |4479346   |430808            |93476          |3757           |3659011           |2703715                   |
|my  |41260   |699731             |354978    |84015             |11120          |139            |460582            |417278                    |
|mzn |17707   |388110             |113356    |17076             |4289           |275            |94895             |77157                     |
|nah |17088   |648422             |102130    |14371             |9360           |9              |56229             |37643                     |
|nap |15652   |804392             |185862    |38250             |3730           |24             |15386             |9569                      |
|nds |34400   |1019548            |739428    |49845             |7998           |               |94701             |83555                     |
|ne  |36048   |776358             |294915    |72685             |6725           |267            |227119            |151236                    |
|new |100918  |1193563            |811948    |77621             |93038          |40             |301295            |278422                    |
|nl  |2554610 |16134593           |31823749  |2764083           |405781         |273            |8918883           |7058397                   |
|nn  |206657  |3652079            |2787235   |361068            |220738         |154            |1064516           |732532                    |
|no  |709542  |7855478            |12087904  |1585810           |472339         |1              |2963577           |1752582                   |
|oc  |98027   |3388435            |1809710   |111315            |56605          |290            |1491784           |1453162                   |
|or  |22359   |736820             |155077    |27071             |13014          |129            |125961            |83250                     |
|os  |20315   |841048             |103564    |14147             |19182          |57             |85549             |74792                     |
|pa  |36168   |891318             |312123    |32395             |10989          |1              |301477            |186180                    |
|pl  |1575762 |13958693           |30792002  |3244389           |639723         |               |11769485          |7345068                   |
|pms |66943   |1802868            |559036    |75942             |17108          |1684           |646961            |642742                    |
|pnb |45336   |1366423            |561884    |102912            |12324          |30             |109729            |89559                     |
|pt  |1667327 |14071743           |26111747  |2373020           |955512         |7566           |7273995           |4220599                   |
|qu  |39107   |1154083            |482813    |50500             |22010          |6              |63522             |48190                     |
|ro  |865444  |8232620            |8715289   |826522            |553650         |8412           |6192337           |4450825                   |
|ru  |3033613 |15350832           |42153229  |3526953           |1291101        |17756          |15382287          |8985787                   |
|sa  |21614   |904789             |153809    |51314             |20169          |69             |56919             |36343                     |
|sah |15801   |712118             |134674    |20229             |16295          |69             |47695             |28315                     |
|scn |41436   |1166819            |317603    |31203             |5130           |2              |27961             |24454                     |
|sco |52878   |2301910            |1023709   |233832            |114366         |4470           |699297            |408021                    |
|sh  |3960865 |6545293            |11431459  |816552            |155263         |2143           |5089465           |3821276                   |
|si  |24285   |463985             |188502    |18755             |13546          |556            |99709             |47463                     |
|sk  |278133  |5968648            |4528315   |392427            |247574         |104            |2370562           |1554812                   |
|sl  |217345  |3989018            |4883529   |467838            |181334         |               |1355263           |859228                    |
|sq  |84404   |1964847            |980879    |120438            |33587          |2985           |428661            |325805                    |
|sr  |873929  |6715446            |7183655   |636686            |135090         |1450           |2073765           |1377521                   |
|su  |22545   |824097             |322313    |36203             |24601          |280            |101861            |76459                     |
|sv  |5858202 |20493578           |63821423  |8408876           |1173139        |299191         |41295967          |38208922                  |
|sw  |51278   |1628706            |591589    |65319             |36065          |214            |151412            |123731                    |
|ta  |125998  |1927015            |1487866   |187259            |61573          |2703           |800562            |452151                    |
|te  |90316   |800772             |1140325   |102931            |31923          |548            |1118134           |994441                    |
|tg  |82736   |1177613            |532675    |121149            |32152          |6517           |424596            |408041                    |
|th  |244223  |3508792            |3246722   |301716            |166312         |3846           |1321855           |566476                    |
|tl  |164847  |2160417            |772135    |88863             |42157          |969            |236413            |139259                    |
|tr  |521200  |7048194            |7850843   |1041967           |700464         |8897           |2991863           |1857031                   |
|tt  |123267  |1789671            |937155    |118971            |56252          |869            |668986            |598652                    |
|uk  |1049249 |10867075           |16777605  |1751135           |542868         |55597          |7691426           |4292580                   |
|ur  |299824  |4365211            |1802815   |227783            |827800         |1326           |2049929           |1582299                   |
|uz  |444074  |3499627            |1061031   |105056            |40227          |45             |2418037           |2371000                   |
|vec |17446   |812239             |174803    |12428             |10141          |43             |115966            |97023                     |
|vi  |1340313 |11542962           |15831995  |2185821           |659046         |5895           |14322161          |11726191                  |
|vo  |242292  |2983432            |971438    |326248            |4556           |237            |543840            |543529                    |
|wa  |28779   |372075             |203900    |9443              |4530           |1              |148               |148                       |
|war |2094871 |10387067           |16711018  |2084543           |445305         |49             |13056238          |13042101                  |
|yi  |24284   |656132             |232315    |20089             |7215           |908            |25925             |22087                     |
|yo  |41250   |1274425            |194529    |32508             |9965           |192            |121360            |70414                     |
|zh  |1620943 |12090949           |23162842  |2220362           |956624         |76187          |8780134           |3868731                   |

|lang  |labels  |categories        |skos           |infobox           |top 100 en infobox        |
|:-----|-------:|-----------------:|--------------:|-----------------:|-------------------------:|
|en-af |35,309  |51,876             |24,897           |391,625            |35,201                     |
|en-als|17,468  |23,019             |7,742            |182,854            |10,734                     |
|en-am |6,600   |3,012              |1,264            |18,094             |2,509                      |
|en-an |26,123  |39,091             |45,955           |310,031            |9,999                      |
|en-ar |300,446 |1,449,091           |641,873          |3,221,789           |551,376                    |
|en-arz|10,387  |21,663             |10,271           |40,954             |8,665                      |
|en-ast|36,925  |52,425             |11,993           |259,835            |37,846                     |
|en-az |69,856  |83,202             |119,335          |514,522            |40,962                     |
|en-azb|4,243   |791               |335             |34,889             |9,268                      |
|en-ba |9,025   |13,600             |43,066           |165,147            |2,014                      |
|en-bar|16,816  |19,160             |15,125           |128,336            |7,623                      |
|en-be |70,479  |123,662            |123,235          |606,574            |38,522                     |
|en-bg |162,125 |291,602            |146,098          |961,885            |132,417                    |
|en-bn |38,017  |68,515             |42,985           |554,805            |181,305                    |
|en-bpy|19,066  |14,251             |16,626           |194,010            |30,096                     |
|en-br |50,395  |71,121             |47,170           |140,924            |16,526                     |
|en-bs |42,104  |71,279             |87,470           |561,564            |35,786                     |
|en-bug|14,092  |14,054             |819             |1,854              |1,571                      |
|en-ca |355,188 |480,650            |108,340          |3,850,141           |224,554                    |
|en-ce |7,709   |2,556              |3,634            |108,573            |920                       |
|en-ceb|349,264 |144,447            |24,776           |4,642,160           |919,702                    |
|en-ckb|15,714  |39,415             |75,827           |99,844             |12,194                     |
|en-cs |240,361 |684,538            |249,121          |2,409,585           |362,289                    |
|en-cv |14,174  |7,806              |5,233            |108,781            |859                       |
|en-cy |59,641  |108,101            |79,036           |3,755,446           |1,755,178                   |
|en-da |149,242 |299,896            |139,019          |1,197,952           |135,317                    |
|en-de |1,018,321|2,874,849           |377,334          |6,715,795           |888,788                    |
|en-el |88,852  |198,997            |109,447          |407,741            |65,627                     |
|en-eo |175,195 |317,953            |137,804          |1,507,094           |8,030                      |
|en-es |812,331 |1,803,738           |639,535          |7,582,341           |492,850                    |
|en-et |89,766  |150,217            |62,727           |349,470            |16,011                     |
|en-eu |203,312 |284,810            |126,505          |1,818,402           |26,922                     |
|en-fa |408,741 |1,998,298           |1,437,440         |5,727,420           |1,179,780                   |
|en-fi |282,776 |640,176            |170,546          |2,839,015           |131,360                    |
|en-fo |10,618  |16,725             |12,332           |40,026             |8,204                      |
|en-fr |1,164,577|2,716,241           |617,089          |10,395,149          |576,593                    |
|en-fy |22,984  |53,636             |47,159           |71,920             |483                       |
|en-ga |35,742  |41,172             |11,477           |65,875             |1,035                      |
|en-gd |12,701  |12,508             |2,422            |16,845             |692                       |
|en-gl |82,424  |173,203            |89,682           |457,845            |37,497                     |
|en-gu |6,733   |9,365              |1,722            |47,133             |10,309                     |
|en-he |149,652 |370,751            |119,963          |1,027,194           |28,781                     |
|en-hi |50,171  |56,192             |45,037           |263,077            |75,607                     |
|en-hr |106,899 |165,019            |38,209           |849,750            |48,572                     |
|en-hsb|7,829   |9,264              |9,595            |66,803             |92                        |
|en-ht |31,848  |43,396             |2,679            |138,019            |38                        |
|en-hu |286,766 |588,257            |114,694          |2,811,827           |342,155                    |
|en-hy |104,196 |123,199            |81,253           |1,394,821           |31,961                     |
|en-ia |16,091  |14,839             |8,056            |999               |107                       |
|en-id |193,462 |252,084            |108,394          |1,532,013           |567,642                    |
|en-io |26,688  |24,460             |16,587           |90,873             |385                       |
|en-is |28,061  |29,340             |31,933           |96,633             |9,349                      |
|en-it |883,922 |880,054            |445,530          |12,916,139          |1,537,143                   |
|en-ja |459,616 |1,511,162           |396,598          |4,333,202           |619,562                    |
|en-jv |22,049  |16,890             |18,332           |179,467            |37,269                     |
|en-ka |78,075  |129,477            |81,541           |920,729            |79,745                     |
|en-kk |93,549  |88,637             |38,027           |2,279,074           |16,080                     |
|en-kn |10,832  |6,042              |2,038            |92,542             |24,490                     |
|en-ko |230,856 |819,163            |551,587          |1,459,320           |98,693                     |
|en-ku |10,503  |12,675             |12,168           |42,037             |2,087                      |
|en-ky |24,137  |9,117              |1,487            |405,913            |2,060                      |
|en-la |115,149 |200,265            |72,573           |350,765            |19,563                     |
|en-lb |30,999  |52,757             |50,660           |97,523             |173                       |
|en-li |8,767   |9,040              |3,524            |8,879              |19                        |
|en-lmo|30,889  |8,933              |3,934            |424,711            |8,083                      |
|en-lt |101,246 |124,981            |51,153           |985,057            |101,212                    |
|en-lv |52,420  |107,290            |63,545           |552,626            |124,933                    |
|en-mg |72,736  |74,857             |2,188            |286,908            |195                       |
|en-min|23,390  |18,292             |950             |83,093             |30,548                     |
|en-mk |58,003  |100,441            |147,302          |434,366            |107,648                    |
|en-ml |33,454  |40,961             |26,651           |321,829            |93,694                     |
|en-mn |13,880  |21,071             |20,703           |142,688            |23,324                     |
|en-mr |31,062  |36,001             |21,497           |274,791            |24,084                     |
|en-mrj|9,354   |12,689             |2,989            |409               |38                        |
|en-ms |201,373 |186,593            |51,723           |2,357,856           |837,640                    |
|en-my |8,926   |10,060             |8,879            |66,039             |15,793                     |
|en-mzn|10,006  |5,551              |2,178            |82,966             |8,111                      |
|en-nah|8,541   |2,837              |3,063            |37,792             |1,213                      |
|en-nap|13,403  |23,478             |2,737            |11,345             |39                        |
|en-nds|19,696  |32,042             |5,836            |71,207             |674                       |
|en-ne |16,382  |16,158             |1,582            |158,432            |61,844                     |
|en-new|21,908  |3,078              |17,935           |138,415            |47,817                     |
|en-nl |781,381 |1,164,505           |242,952          |5,126,717           |835,924                    |
|en-nn |104,700 |224,615            |153,096          |912,389            |55,215                     |
|en-no |307,842 |894,128            |312,354          |1,976,148           |75,057                     |
|en-oc |78,455  |70,688             |34,738           |1,413,692           |1,443                      |
|en-or |9,177   |11,895             |8,619            |89,095             |33,645                     |
|en-os |7,122   |7,300              |12,977           |60,487             |1,461                      |
|en-pa |17,558  |8,507              |3,014            |157,912            |42,070                     |
|en-pl |751,845 |1,504,459           |288,359          |7,917,683           |472,984                    |
|en-pms|57,198  |55,891             |7,394            |612,319            |96,133                     |
|en-pnb|34,231  |13,071             |6,142            |38,643             |12,523                     |
|en-pt |660,417 |1,415,211           |505,330          |5,514,854           |558,741                    |
|en-qu |16,173  |28,204             |14,590           |45,351             |3,550                      |
|en-ro |248,287 |451,192            |284,740          |3,590,140           |683,828                    |
|en-ru |708,594 |1,597,861           |543,809          |8,982,662           |1,100,867                   |
|en-sa |8,014   |17,629             |14,310           |41,573             |13,506                     |
|en-sah|7,031   |4,815              |5,954            |35,263             |7,780                      |
|en-scn|20,984  |21,571             |2,915            |22,583             |426                       |
|en-sco|40,440  |102,229            |92,024           |596,071            |207,142                    |
|en-sh |209,987 |302,447            |115,619          |2,293,914           |190,468                    |
|en-si |8,113   |1,770              |1,827            |61,447             |17,752                     |
|en-sk |150,113 |225,967            |151,535          |1,879,808           |100,693                    |
|en-sl |98,948  |236,504            |118,556          |1,096,815           |421,950                    |
|en-sq |39,881  |50,179             |17,896           |261,983            |22,605                     |
|en-sr |230,941 |372,975            |88,339           |1,535,586           |266,585                    |
|en-su |9,407   |4,942              |5,071            |39,573             |7,045                      |
|en-sv |706,492 |1,505,243           |432,434          |6,617,536           |676,907                    |
|en-sw |27,096  |37,589             |27,615           |124,131            |50,792                     |
|en-ta |57,327  |87,782             |30,458           |571,893            |130,296                    |
|en-te |17,156  |17,000             |7,655            |173,949            |48,393                     |
|en-tg |30,285  |21,488             |13,997           |196,643            |4,512                      |
|en-th |77,688  |162,113            |117,610          |860,446            |236,997                    |
|en-tl |51,964  |46,678             |22,677           |193,284            |49,592                     |
|en-tr |192,817 |651,953            |452,089          |1,923,748           |381,663                    |
|en-tt |16,207  |16,943             |31,525           |115,352            |1,187                      |
|en-uk |372,708 |813,905            |270,834          |4,865,331           |399,708                    |
|en-ur |91,216  |178,833            |746,135          |1,750,242           |691,392                    |
|en-uz |88,037  |58,237             |20,683           |2,214,101           |13,286                     |
|en-vec|9,770   |6,777              |5,808            |103,423            |176                       |
|en-vi |427,099 |751,492            |269,451          |6,021,715           |1,657,255                   |
|en-vo |116,967 |259,396            |2,411            |523,047            |87                        |
|en-wa |4,799   |3,801              |2,612            |117               |25                        |
|en-war|270,261 |157,361            |62,745           |1,536,543           |433,060                    |
|en-yi |9,393   |12,299             |5,074            |20,098             |653                       |
|en-yo |29,244  |27,016             |8,254            |92,121             |28,781                     |
|en-zh |497,704 |834,988            |386,090          |5,521,741           |1,587,672                   |
