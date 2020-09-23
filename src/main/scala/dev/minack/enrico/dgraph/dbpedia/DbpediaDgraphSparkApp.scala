package dev.minack.enrico.dgraph.dbpedia

import java.io.File

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import Helpers.ConditionalDataFrame

object DbpediaDgraphSparkApp {

  def main(args: Array[String]): Unit = {

    println("This tool pre-processes all selected languages from the datasets into Dgraph-compatible RDF files.")

    if (args.length < 2 || args.length > 3) {
      println()
      println("Please provide path to dbpedia dataset, the release and optionally languages (two-letters code, comma separated)")
      println("The set of languages can be a subset of the languages given to SbpediaToParquetSparkApp")
      System.exit(1)
    }

    val base = args(0)
    val release = args(1)
    val dataset = "core-i18n"
    val languages = if (args.length == 3) args(2).split(",").toSeq else getLanguages(base, release, dataset)
    val externaliseUris = true
    val removeLanguageTags = false

    println(s"Pre-processing release $release of $dataset")
    println(s"Pre-processing these languages: ${languages.mkString(", ")}")
    if (externaliseUris)
      println("URIs will be externalized")
    if (removeLanguageTags)
      println("Language tags will be removed from string literals")

    val start = System.nanoTime()

    // start a local Spark session
    implicit val spark: SparkSession =
      SparkSession
        .builder()
        .master("local[*]")
        .appName("Spark Dgraph DBpedia App")
        .config("spark.local.dir", ".")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.showConsoleProgress", "true")
        .getOrCreate()
    import spark.implicits._

    // defines some useful column functons
    val blank =
      if (externaliseUris)
        (c: String) => concat(lit("_:"), md5(col(c))).as(c)
      else
        (c: String) => col(c)
    val removeLangTag =
      if (removeLanguageTags)
        regexp_replace(col("o"), "@[a-z]+$", "").as("o")
      else
        col("o")

    // load files from parquet, only these datasets are pre-processed
    val labelTriples = readParquet(s"$base/$release/$dataset/labels.parquet").where($"lang".isin(languages: _*))
    val infoboxTriples = readParquet(s"$base/$release/$dataset/infobox_properties.parquet").where($"lang".isin(languages: _*))
    val interlangTriples = readParquet(s"$base/$release/$dataset/interlanguage_links.parquet").where($"lang".isin(languages: _*))
    val categoryTriples = readParquet(s"$base/$release/$dataset/article_categories.parquet").where($"lang".isin(languages: _*))

    // print some stats, they are not too costly to print
    Seq(
      "labels" -> labelTriples,
      "infoboxe_properties" -> infoboxTriples,
      "interlanguage_links" -> interlangTriples,
      "article_categories" -> categoryTriples
    ).foreach { case (label, df) =>
      println(s"$label: ${df.count} triples, ${df.select($"s").distinct().count} nodes, ${df.select($"p").distinct().count} predicates")
    }

    // define labels without language tag (if removeLanguageTags is true)
    val labels =
      labelTriples
        .withColumn("s", blank("s"))
        .withColumn("o", removeLangTag)

    // all datatypes other than these will be interpreted as <http://www.w3.org/2001/XMLSchema#string>
    val supportedDataTypes = Seq(
      "<uri>",
      "<http://www.w3.org/2001/XMLSchema#date>",
      "<http://www.w3.org/2001/XMLSchema#double>",
      "<http://www.w3.org/2001/XMLSchema#integer>",
      "<http://www.w3.org/2001/XMLSchema#string>",
    )
    // this is deterministic, but marking it non-deterministic guarantees it is executed only once per row
    val extractDataTypeUdf = udf(extractDataType(_)).asNondeterministic()

    // add data type to infobox properties, replace data type with known ones
    val infoboxTriplesWithDataType =
      infoboxTriples
        .withColumn("o+t", extractDataTypeUdf($"o"))
        .select($"s", $"p", $"o+t".getItem(0).as("v"), $"o+t".getItem(1).as("t"), $"lang")
        .withColumn("t", when($"t".isin(supportedDataTypes: _*), $"t").otherwise("<http://www.w3.org/2001/XMLSchema#string>"))

    // get most frequent data type per property
    val infoboxPropertyDataType =
      infoboxTriplesWithDataType
        .groupBy($"p", $"t").count()
        .withColumn("freq", row_number() over Window.partitionBy($"p").orderBy($"count".desc))
        .where($"freq" === 1)
        .select($"p", $"t")

    // infobox properties with most frequent data type per property
    val infobox =
      infoboxTriplesWithDataType
        .join(infoboxPropertyDataType, Seq("p", "t"))
        // negative years not supported by dgraph load
        .where($"t" =!= "<http://www.w3.org/2001/XMLSchema#date>" || !$"v".startsWith("\"-"))
        .withColumn("s", blank("s"))
        .withColumn("v", when($"t" === "<uri>", blank("v")).otherwise(col("v")))
        .select($"s", $"p", when($"t" === "<uri>", $"v").otherwise(concat($"v", lit("^^"), $"t")).as("o"), $"lang")

    // interlanguage links preprocessing
    // we are only interested in links inside our set of languages
    // we look at the dbpedia urls, en links may not contain the language code in the url,
    // but we expect `db` at its place, so with `en` in languages, we also look for links with `db`
    val langs = languages ++ (if (languages.contains("en")) Seq("db") else Seq.empty[String])
    val interlang =
      interlangTriples
        .where($"o".substr(9, 2).isin(langs: _*))
        .withColumn("s", blank("s"))
        .withColumn("o", blank("o"))

    // article_categories
    val categories =
      categoryTriples
        .withColumn("s", blank("s"))
        .withColumn("o", blank("o"))

    // xid predicate
    val xid = Seq(("<xid>", "any", "string", "@index(exact)")).toDF("p", "lang", "t", "i")

    // mapping to Dgraph types
    val dgraphDataTypes = Map(
      "<uri>" -> "uid",
      "<http://www.w3.org/2001/XMLSchema#date>" -> "datetime",
      "<http://www.w3.org/2001/XMLSchema#double>" -> "float",
      "<http://www.w3.org/2001/XMLSchema#integer>" -> "int",
      "<http://www.w3.org/2001/XMLSchema#string>" -> "string",
    )
    // this is deterministic, but marking it non-deterministic guarantees it is executed only once per row
    val dgraphDataTypesUdf = udf(dgraphDataTypes(_)).asNondeterministic()

    // mapping to Dgraph indices
    val dgraphIndices = Map(
      "uid" -> "@reverse",
      "datetime" -> "@index(day)",
      "float" -> "@index(float)",
      "int" -> "@index(int)",
      "string" -> (if(removeLanguageTags) "@index(fulltext)" else "@lang @index(fulltext)"),
    )
    val dgraphIndicesUdf = udf(dgraphIndices(_)).asNondeterministic()

    // get all predicates from the datasets
    val predicates =
      Seq(
        labelTriples.select($"p", lit("any").as("lang"), lit("string").as("t"), lit("@index(fulltext)").as("i")),
        infoboxTriples.join(infoboxPropertyDataType, "p").withColumn("t", dgraphDataTypesUdf($"t")).select($"p", $"lang", $"t", dgraphIndicesUdf($"t").as("i")),
        interlangTriples.select($"p", lit("any").as("lang"), lit("uid").as("t"), lit("@reverse").as("i")),
        categoryTriples.select($"p", lit("any").as("lang"), lit("uid").as("t"), lit("@reverse").as("i")),
      ).reduce(_.unionByName(_))
        .distinct()
        .optional(externaliseUris, _.unionByName(xid))
        .sort()
        .coalesce(1)

    // TODO: two schema files, one with indices, one without
    // string: lang, index: fulltext
    // datetime: index: year, month, day, hour
    // float: index: float
    // int: index: int
    // uid: reverse

    // write schema without indices
    predicates
      .select(concat($"p", lit(": "), $"t", lit(" .")).as("p"), $"lang")
      .write
      .partitionBy("lang")
      .mode(SaveMode.Overwrite)
      .text(s"$base/$release/$dataset/schema.dgraph")

    // write schema with indices
    predicates
      .select(concat($"p", lit(": "), $"t", lit(" "), $"i", lit(" .")).as("p"), $"lang")
      .write
      .partitionBy("lang")
      .mode(SaveMode.Overwrite)
      .text(s"$base/$release/$dataset/schema.indexed.dgraph")

    // get all types from the datasets
    val articlesTypes =
      Seq(
        labelTriples.select(blank("s"), $"lang"),
        infoboxTriples.select(blank("s"), $"lang"),
        interlangTriples.select(blank("s"), $"lang").where($"o".substr(9, 2).isin(langs: _*)),
        interlangTriples.select(blank("o").as("s"), $"lang").where($"o".substr(9, 2).isin(langs: _*)),
        categoryTriples.select(blank("s"), $"lang")
      )
        .map(_.distinct())
        .reduce(_.unionByName(_))
        .withColumn("p", lit("<dgraph.type>"))
        .withColumn("o", lit("\"Article\""))
    val categoryTypes =
      categoryTriples
        .select(blank("o").as("s"), $"lang").distinct()
        .withColumn("p", lit("<dgraph.type>"))
        .withColumn("o", lit("\"Category\""))
    val types =
      articlesTypes
        .unionByName(categoryTypes)
        .distinct()

    val externalIds =
      Seq(
        labelTriples.select($"s", $"lang"),
        infoboxTriples.select($"s", $"lang"),
        interlangTriples.select($"s", $"lang").where($"o".substr(9, 2).isin(langs: _*)),
        interlangTriples.select($"o".as("s"), $"lang").where($"o".substr(9, 2).isin(langs: _*)),
        categoryTriples.select($"s", $"lang"),
        categoryTriples.select($"o".as("s"), $"lang")
      ).reduce(_.unionByName(_))
        .distinct()
        .select(
          blank("s"),
          lit("<xid>").as("p"),
          concat(lit("\""), $"s".substr(lit(2), length($"s")-2), lit("\"")).as("o"),
          $"lang"
        )

    // write dgraph rdf files
    writeRdf(labels, s"$base/$release/$dataset/labels.rdf")
    writeRdf(infobox, s"$base/$release/$dataset/infobox_properties.rdf")
    writeRdf(interlang.toDF, s"$base/$release/$dataset/interlanguage_links.rdf")
    writeRdf(categories.toDF, s"$base/$release/$dataset/article_categories.rdf")
    writeRdf(types.toDF, s"$base/$release/$dataset/types.rdf")
    writeRdf(externalIds.toDF, s"$base/$release/$dataset/external_ids.rdf")

    val infoboxRdf = spark.read.text(s"$base/$release/$dataset/infobox_properties.rdf")
    println(s"cleaned-up infoboxes cover ${infoboxRdf.count() * 100 / infoboxTriples.count()}% of original rows")
    val duration = (System.nanoTime() - start) / 1000000000
    println(s"finished in ${duration / 3600}h ${(duration / 60) % 60}m ${duration % 60}s")
  }

  def getLanguages(base: String, release: String, dataset: String): Seq[String] =
    new File(new File(new File(base), release), dataset)
      .listFiles().toSeq
      .filter(_.isDirectory)
      .map(_.getName)
      .filter(_.length == 2)

  def readParquet(path: String)(implicit spark: SparkSession): Dataset[Triple] = {
    import spark.implicits._
    spark.read.parquet(path).as[Triple]
  }

  def writeRdf(df: DataFrame, path: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    // @ and ~ not allowed in predicates in Dgraph
    df.where(!$"p".contains("@") && !$"p".contains("~"))
      // with this range partition and sort you get fewer partitions
      // for smaller languages and order within your partitions
      .repartitionByRange($"lang", $"p", $"s")
      .sort("lang", "p", "s", "o")
      .select(concat($"s", lit(" "), $"p", lit(" "), $"o", lit(" .")), $"lang")
      .write
      .option("compression", "gzip")
      .mode(SaveMode.Overwrite)
      // this partitioning allows you to read in a subset of languages efficiently
      .partitionBy("lang")
      .text(path)
  }

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

}

case class Triple(s: String, p: String, o: String)

object Helpers {

  implicit class ConditionalDataFrame(df: DataFrame) {
    def optional(condition: Boolean, method: DataFrame => DataFrame): DataFrame =
      if (condition) method(df) else df
  }

}
