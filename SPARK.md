# Graph Pre-processing with SPARK

This project uses [Apache Spark](https://spark.apache.org/) to bring real-work graph data into a Dgraph-compatible shape.

## A large real-world dataset 
I was looking for a large real-world graph dataset to load into a Dgraph cluster to ultimately test
my [dgraph-spark-connector](https://github.com/G-Research/spark-dgraph-connector).
Dgraph organizes the graph around predicates, so that dataset should contain predicates with a few characteristics:

- numerous predicates, to have a large real-world schema
- a predicate with a lot of data, ideally a long string that exists for every node
- a long-tail predicate frequency distribution: a few predicates have high frequency (and low selectivity), most predicates have low frequency (and high selectivity)
- predicates that, if they exist for a node:
  - have a single occurrence (single value)
  - have a multiple occurrences (value list)
- real-world predicate names in multiple languages
- various data types and strings in multiple languages

A good dataset that checks all these boxes can be found at the [DBpedia project](https://wiki.dbpedia.org/).
They extract structured information from the [Wikipedia project](https://wikipedia.org/) and provides them in RDF format.
However, that RDF data requires some preparation before it can be loaded into Dgraph.
Given the size of the datasets, a scalable pre-processing step is required.

This article outlines this pre-processing done with [Apache Spark](https://spark.apache.org/).
Spark is a big data processing platform that splits row-based data into smaller partitions,
distributes them across a cluster to transforms your data.

## Datasets

This tutorial uses the following datasets from [DBpedia project](https://wiki.dbpedia.org/):

|dataset             |filename                        |description|
|--------------------|--------------------------------|-----------|
|labels              |`labels_{lang}.ttl`             |Each article has a single title in the article's language.|
|category            |`article_categories_{lang}.ttl` |Some articles link to categories, multiple categories allowed.|
|inter-language links|`interlanguage_links_{lang}.ttl`|Articles link to the same article in all other languages.|
|infobox             |`infobox_properties_{lang}.ttl` |Some articles have infoboxes. This are key-value tables that provide structured information.|

The `infobox` dataset provides real-world user-generated multi-language predicates.
The other datasets provide a single predicate each.

## Datasets Preparations

We will see the following transformations and cleanups to get to a Dgraph-compatible dataset:

- [Read](#reading-ttl-files) and [write](#writing-rdf-files) `.ttl` files
- [Extract infobox data types](#extract-data-types)
- [Disambiguate infobox data types](#disambiguate-infobox-data-types)
- [Take most frequent infobox predicates](#most-frequent-infobox-predicates)
- [General data cleanup](#general-data-cleanup)
- [Generate Dgraph schema](#generate-dgraph-schema)

And optionally:

- [Handle external ids](#external-ids) (generate blank nodes, produce `<xid>` triples)
- [Remove language tags from strings](#remove-language-tags)

## Reading .ttl files

A single line of a DBpedia `.ttl` files look like this:

    <http://de.dbpedia.org/resource/Alan_Smithee> <http://de.dbpedia.org/property/typ> "p"^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#langString> .

Read such a `.ttl` file from `path` as a `Dataset[Triple]`: 

    case class Triple(s: String, p: String, o: String)

    def readTtl(path: String)(implicit spark: SparkSession): Dataset[Triple] = {
      import spark.implicits._

      spark

        // read the .ttl file as a text file
        .read.textFile(path)

        // ignore lines that start with #
        .where(!$"value".startsWith("#"))

        // remove the last two characters (' .') from the ttl lines
        // and split at the first two spaces (three columns: subject, predicate, object)
        .map(line => line.dropRight(2).split(" ", 3))

        // get the three columns `s`, `p` and `o`
        .select($"value"(0).as("s"), $"value"(1).as("p"), $"value"(2).as("o"))

        // type the DataFrame as Dataset[Triple]
        .as[Triple]
    }

The `Dataset[Triple]` returned by that method contains above line as:

|s                                            |p                                   |o                                                           |
|---------------------------------------------|------------------------------------|------------------------------------------------------------|
|<http://de.dbpedia.org/resource/Alan_Smithee>|<http://de.dbpedia.org/property/typ>|"p"^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#langString>|

All three columns are of type `String`, where subject `s` and predicate `p` represent URIs, object `o` represents
either a URI, or a literal of various types with optional data type and language attributes.

Each language has its own file for each dataset. For instance, the `labels` dataset is split into
`labels_en.ttl`, `labels_de.ttl`, …

We now read each language file, add a `lang` column and union all together to a single `DataFrame`:

    val base = "dbpedia"
    val release = "2016-10"
    val dataset = "core-i18n"
    val extension = ".ttl"

    val filename = "infobox_properties"
    val languages = Seq("en", "de", …)
    
    val triples =
      languages.map(lang =>
        readTtl(s"$base/$release/$dataset/$lang/${filename}_$lang$extension")
          .withColumn("lang", lit(lang))
      )
        // union all ttl files
        .reduce(_.unionByName(_))

## Extract Data Types

For further processing of the object column we need to strip the data type information from the value read from the `.ttl` file.
The following method returns an array containing the value and the type:

    def extractDataType(value: String): Array[String] = {
      if (value.startsWith("<")) {
        Array(value, "<uri>")
      } else if (value.contains("^^")) {
        val fields = value.split("\\^")
        Array(fields.dropRight(2).mkString("^"), fields.last)
      } else {
        Array(value)
      }
    }

An example input string `"p"^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#langString>` is split into
an array of `"p"` and `<http://www.w3.org/1999/02/22-rdf-syntax-ns#langString>`.

An input string that represents a URI like `<http://de.dbpedia.org/resource/Alan_Smithee>` is split into
an array of `<http://de.dbpedia.org/resource/Alan_Smithee>` and `<uri>`.

This method can be applied on the ttl `DataFrame` as follows:

    // this is deterministic, but marking it non-deterministic guarantees it is executed only once per row
    val extractDataTypeUdf = udf(extractDataType(_)).asNondeterministic()

    val triplesWithDataType =
      triples

        // populate column `o+t` from `o` containing the array of value and data type
        .withColumn("o+t", extractDataTypeUdf($"o"))

        // get the columns `s`, `p`, `v` (pure value), `t` (data type URI) and `lang`
       .select($"s", $"p", $"o+t".getItem(0).as("v"), $"o+t".getItem(1).as("t"), $"lang")

Any value with a data type that is not supported by Dgraph (e.g. `<http://www.w3.org/1999/02/22-rdf-syntax-ns#langString>`)
should be interpreted as `<http://www.w3.org/2001/XMLSchema#string>`:

    // all datatypes other than these will be interpreted as <http://www.w3.org/2001/XMLSchema#string>
    val supportedDataTypes = Seq(
      "<uri>",
      "<http://www.w3.org/2001/XMLSchema#date>",
      "<http://www.w3.org/2001/XMLSchema#double>",
      "<http://www.w3.org/2001/XMLSchema#integer>",
      "<http://www.w3.org/2001/XMLSchema#string>",
    )

    val triplesWithDgraphDataType =
      triplesWithDataType

        // replace all data types that are not supported by Dgraph with `<http://www.w3.org/2001/XMLSchema#string>`
        .withColumn("t", when($"t".isin(supportedDataTypes: _*), $"t").otherwise("<http://www.w3.org/2001/XMLSchema#string>"))


|s                                            |p                                   |v  |t                                        |lang|
|---------------------------------------------|------------------------------------|---|-----------------------------------------|----|
|<http://de.dbpedia.org/resource/Alan_Smithee>|<http://de.dbpedia.org/property/typ>|"p"|<http://www.w3.org/2001/XMLSchema#string>|de  |

## Disambiguate Infobox Data Types

The infobox dataset contains user-generated predicates and values.
The data type of predicates is not consistent across articles (triple subjects).
We can pick the most frequent data type per predicate and drop all conflicting triples:

    val infoboxPropertyDataType =
      infoboxTriplesWithDataType

        // count how often a type is used by each predicate
        .groupBy($"p", $"t").count()

        // rank the types for each predicate in ascending order
        // for types with the same count, pick the one with the type earlier in the alphabeth
        // this guarantees the result to be deterministic
        .withColumn("k", row_number() over Window.partitionBy($"p").orderBy($"count".desc, $"t"))

        // only pick the most frequent type
        .where($"k" === 1)

        // select interesting columns
        .select($"p", $"t")

        // we can cache this DataFrame as it is expected to be quite small
        .cache()

We can now filter the infobox triples `DataFrame` for these predicates and types:

    val infobox =
      infoboxTriplesWithDataType

        // filter with most-frequent type of each predicate
        .join(infoboxPropertyDataType, Seq("p", "t"), "semi_join")


## Most Frequent Infobox Predicates

The `infobox` dataset contains over 400.000 predicates across all languages. This is a great source
for a real-world dataset with a large schema. But if you are looking for a medium size schema, you want
to pick the most-frequent infobox properties from each language and get a schema that is more similar (in size)
across the languages.

    val topk = 100
    
    // get the top-k most frequent properties per language
    val topkProperties =
      infoboxTriples

        // count how often a predicate occurs in each language
        .groupBy($"p", $"lang").count()

        // rank the predicates for each language in ascending order
        // for predicates with the same count, pick the one earlier in the alphabeth
        // this guarantees the result to be deterministic
        .withColumn("k", row_number() over Window.partitionBy($"lang").orderBy($"count".desc, $"p"))

        // only pick the most frequent predicates
        .where($"k" <= topk)

        // select interesting columns
        .select($"p", $"lang")

        // we can cache this DataFrame as it is expected to be quite small
        .cache()

    val topkInfoboxTriples =
      triples

        // filter triples for top-k most frequent properties per language
        .join(topkProperties, Seq("p", "lang"), "left_semi")

## General data cleanup

The `infobox` dataset contains predicates and values that are not supported by Dgraph.
Those triples need to be filtered out:

    infoboxTriples
    
      // @ and ~ not allowed in predicates in Dgraph
      .where(!$"p".contains("@") && !$"p".contains("~"))

      // negative years not supported by Dgraph
      .where($"t" =!= "<http://www.w3.org/2001/XMLSchema#date>" || !$"v".startsWith("\"-"))

## Generate Dgraph schema

Before we can load the triples into Dgraph, we need to define a schema that contains all
predicates. This can be generated from our datasets:

    // define all predicates from our four datasets
    // for each dataset we provide: `p`: the predicate, `lang`: its language, `t`: its Dgraph data type, `i`: indices
    val predicates =
      Seq(
        // labels are always strings with fulltext index
        labelTriples.select($"p", lit("any").as("lang"), lit(s"string${lang}").as("t"), lit("@index(fulltext)").as("i")),

        // infobox properties data type and index depends on their data type `t`
        infoboxTriples.join(infoboxPropertyDataType, "p").withColumn("t", dgraphDataTypesUdf($"t")).select($"p", $"lang", $"t", dgraphIndicesUdf($"t").as("i")),

        // interlanguage links are always uri lists with reverse index
        interlangTriples.select($"p", lit("any").as("lang"), lit("[uid]").as("t"), lit("@reverse").as("i")),

        // categories are always uri lists with reverse index
        categoryTriples.select($"p", lit("any").as("lang"), lit("[uid]").as("t"), lit("@reverse").as("i")),
      )

        // union all datasets
        .reduce(_.unionByName(_))

        // we are only interested in one line per predicate
        .distinct()

        // get a nicely sorted file
        .sort()

        // cache this, because it is small and we want to write it twice but generate only once
        .cache()

        // get a single file
        .coalesce(1)

We can write the schema now once with indices:

    // write schema with indices
    predicates
      .select(concat($"p", lit(": "), $"t", lit(" "), $"i", lit(" .")).as("p"), $"lang")
      .mode(SaveMode.Overwrite)
      .text(s"$base/$release/$dataset/schema.indexed.dgraph")

… and once without indices, if we wish:

    // write schema without indices
    predicates
      .select(concat($"p", lit(": "), $"t", lit(" .")).as("p"), $"lang")
      .mode(SaveMode.Overwrite)
      .text(s"$base/$release/$dataset/schema.dgraph")

## External ids

    val externaliseUris = false

    val blank = (c: String) => concat(lit("_:"), md5(col(c))).as(c)

    val labels =
      labelTriples
        .conditionally(externaliseUris, _.withColumn("s", blank("s")))
        .conditionally(removeLanguageTags, _.withColumn("o", removeLangTag))

    val infobox =
      infoboxTriplesWithDataType
        .conditionally(externaliseUris, _.withColumn("s", blank("s")))
        .conditionally(externaliseUris, _.withColumn("v", when($"t" === "<uri>", blank("v")).otherwise(col("v"))))

    // article_categories
    val categories =
      categoryTriples
        .conditionally(externaliseUris, _.withColumn("s", blank("s")))
        .conditionally(externaliseUris, _.withColumn("o", blank("o")))

## Remove Language Tags

    val removeLanguageTags = false

    val removeLangTag = regexp_replace(col("o"), "@[a-z]+$", "").as("o")

    val labels =
      labelTriples
        .conditionally(removeLanguageTags, _.withColumn("o", removeLangTag))


## Writing RDF files

## Scala Spark helpers

### Conditional Transformation

    implicit class ConditionalDataFrame[T](df: Dataset[T]) {
      def conditionally(condition: Boolean, method: DataFrame => DataFrame): DataFrame =
        if (condition) method(df.toDF) else df.toDF
    }

### Even-sized Partitions for variable-sized Languages
   
    implicit class PartitionedDataFrame[T](df: Dataset[T]) {
      // with this range partition and sort you get fewer partitions
      // for smaller languages and order within your partitions
      // the partitionBy allows you to read in a subset of languages efficiently
      def writePartitionedBy(hadoopPartitions: Seq[String],
                             fileIds: Seq[String],
                             fileOrder: Seq[String] = Seq.empty,
                             projection: Option[Seq[Column]] = None): DataFrameWriter[Row] = {
        df
          .repartitionByRange((hadoopPartitions ++ fileIds).map(col): _*)
          .sortWithinPartitions((hadoopPartitions ++ fileIds ++ fileOrder).map(col): _*)
          .conditionally(projection.isDefined, _.select(projection.get: _*))
          .write
          .partitionBy(hadoopPartitions: _*)
      }
    }

equivalent code for `writePartitionedBy`
