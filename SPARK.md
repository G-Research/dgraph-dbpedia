# Graph Pre-processing with SPARK

This article outlines the pre-processing done with [Apache Spark](https://spark.apache.org/).
Spark is a big data processing platform that splits row-based data into smaller partitions,
distributes them across a cluster to transforms your data.

## Datasets Preparations

We will see the following transformations and cleanups to get to a Dgraph-compatible dataset:

- [Read](#reading-ttl-files) and [write](#writing-rdf-files) `.ttl` files
- [Extract predicate data types](#extract-data-types)
- [Disambiguate predicate data types](#disambiguate-infobox-data-types)
- [Take most frequent predicates](#most-frequent-infobox-predicates)
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

The unioned `DataFrame` looks like this then:

|s                                            |p                                       |o                                                                                |lang|
|---------------------------------------------|----------------------------------------|---------------------------------------------------------------------------------|----|
|<http://dbpedia.org/resource/A>              |<http://dbpedia.org/property/name>      |"Latin Capital Letter A"^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#langString>|en  |
|<http://de.dbpedia.org/resource/Alan_Smithee>|<http://de.dbpedia.org/property/typ>    |"p"^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#langString>                     |de  |
|<http://vi.dbpedia.org/resource/Internet_Society>|<http://vi.dbpedia.org/property/tên>|"Internet Society"^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#langString>      |vi  |


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
        .join(infoboxPropertyDataType, Seq("p", "t"), "left_semi")


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

This clean-up removes ~10% of infobox triples.

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
predicates. This can be generated from our datasets.

We first define the indices that we want for each Dgraph data type:

    // mapping to Dgraph indices
    val dgraphIndices = Map(
      "uid" -> "@reverse",
      "[uid]" -> "@reverse",
      "datetime" -> "@index(day)",
      "float" -> "@index(float)",
      "int" -> "@index(int)",
      "string" -> "@index(fulltext)",
    )
    val dgraphIndicesUdf = udf(dgraphIndices(_)).asNondeterministic()

Now we can generate a Dgraph schema line from each predicate that exists in our datasets:

    // define all predicates from our four datasets
    // for each dataset we provide: `p`: the predicate, `lang`: its language, `t`: its Dgraph data type, `i`: indices
    val predicates =
      Seq(
        // labels are always strings with fulltext index
        labelTriples.select($"p", lit("any").as("lang"), lit(s"string").as("t"), lit("@index(fulltext)").as("i")),

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
      // turn columns into schema line: "$p: $t $i .",
      // e.g. "<http://de.dbpedia.org/property/typ>: string @index(fulltext) ."
      .select(concat($"p", lit(": "), $"t", lit(" "), $"i", lit(" .")).as("p"), $"lang")
      .mode(SaveMode.Overwrite)
      .text(s"$base/$release/$dataset/schema.indexed.dgraph")

… and once without indices, if we wish:

    // write schema without indices
    predicates
      // turn columns into schema line: "$p: $t $i .",
      // e.g. "<http://de.dbpedia.org/property/typ>: string @index(fulltext) ."
      .select(concat($"p", lit(": "), $"t", lit(" .")).as("p"), $"lang")
      .mode(SaveMode.Overwrite)
      .text(s"$base/$release/$dataset/schema.dgraph")

## External ids

Dgraph does not support [external IDs](https://dgraph.io/docs/mutations/external-ids/) (URIs)
as node identifiers. The recommended approach to preserve node URIs is to store them in the `<xid>` predicate.
The Dgraph [bulk](https://dgraph.io/docs/deploy/fast-data-loading/#bulk-loader)
and [live](https://dgraph.io/docs/deploy/fast-data-loading/#live-loader) loaders
can convert external IDs for you. If you wish to generate an RDF dataset that does not use URIs
as node identifiers but blank nodes, you can use the following approach.
It defines a method `blank` that takes a column name and returns a `Column` that turns
the given column (here all URI columns) into blank nodes:

    val blank = (c: String) => concat(lit("_:"), md5(col(c))).as(c)

    val labels =
      labelTriples
        .withColumn("s", blank("s"))
        .withColumn("o", removeLangTag)

    val infobox =
      infoboxTriplesWithDataType
        .withColumn("s", blank("s"))
        .withColumn("v", when($"t" === "<uri>", blank("v")).otherwise(col("v")))

    // article_categories
    val categories =
      categoryTriples
        .withColumn("s", blank("s"))
        .withColumn("o", blank("o"))

We can generate the `<xid>` predicates for all datasets as follows:

    val externalIds =
      Seq(
        labelTriples.select($"s", $"lang"),
        infoboxTriples.select($"s", $"lang"),
        interlangTriples.select($"s", $"lang"),
        interlangTriples.select($"o".as("s"), $"lang")),
        categoryTriples.select($"s", $"lang"),
        categoryTriples.select($"o".as("s"), $"lang")
      )
        .map(_.distinct())
        .reduce(_.unionByName(_))
        .distinct()
        .select(
          blank("s"),
          lit("<xid>").as("p"),
          concat(lit("\""), $"s".substr(lit(2), length($"s")-2), lit("\"")).as("o"),
          $"lang"
        )

## Remove Language Tags

We can remove language tags from triple values with a regular expression:

    val removeLangTag = regexp_replace(col("o"), "@[a-z]+$", "").as("o")

    val labels = labelTriples.withColumn("o", removeLangTag)

This turns literals like `"Alan Smithee"@de` into `"Alan Smithee"`.

## Writing RDF files

Finally, we want to store our processed triples into `.ttl` RDF files.:

    triples
      .select(concat($"s", lit(" "), $"p", lit(" "), $"o", lit(" .")), $"lang")
      .write
      .partitionBy("lang")
      .option("compression", "gzip")
      .text(path)
