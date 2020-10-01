package dgraph.dbpedia

import java.io.File
import java.util.concurrent.atomic.AtomicLong

import dgraph.dbpedia.Helpers.{ConditionalDataFrame, PartitionedDataFrame}
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, DataFrameReader, DataFrameWriter, Dataset, Row, SaveMode, SparkSession}

object DbpediaDgraphSparkApp {

  def main(args: Array[String]): Unit = {

    println("This tool pre-processes all selected languages from the datasets into Dgraph-compatible RDF files.")

    if (args.length < 2 || args.length > 3) {
      println()
      println("Please provide path to dbpedia dataset, the release and optionally languages (two-letters code, comma separated)")
      println("The set of languages can be a subset of the languages given to DbpediaToParquetSparkApp")
      System.exit(1)
    }

    val base = args(0)
    val release = args(1)
    val dataset = "core-i18n"
    val languages = if (args.length == 3) args(2).split(",").toSeq else getLanguages(base, release, dataset)
    val externaliseUris = false
    val removeLanguageTags = false
    // set to None to get all infobox properties, or Some(100) to get top 100 infobox properties
    val topInfoboxPropertiesPerLang = Some(100)
    val printStats = true

    println(s"Pre-processing release $release of $dataset")
    println(s"Pre-processing these languages: ${languages.mkString(", ")}")
    if (externaliseUris)
      println("URIs will be externalized")
    if (removeLanguageTags)
      println("Language tags will be removed from string literals")
    if (topInfoboxPropertiesPerLang.isDefined)
      println(s"Will take only the ${topInfoboxPropertiesPerLang.get} largest infobox properties per language")
    println()

    val start = System.nanoTime()

    // start a local Spark session
    implicit val spark: SparkSession =
      SparkSession
        .builder()
        .master("local[*]")
        .appName("Spark Dgraph DBpedia App")
        .config("spark.local.dir", ".")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    import spark.implicits._

    val memSpilled = new AtomicLong()
    val diskSpilled = new AtomicLong()
    val peakMem = new AtomicLong()
    val listener = new SparkListener {
      override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
        val metrics = stageCompleted.stageInfo.taskMetrics
        memSpilled.addAndGet(metrics.memoryBytesSpilled)
        diskSpilled.addAndGet(metrics.diskBytesSpilled)
        peakMem.getAndUpdate((l: Long) => math.max(l, metrics.peakExecutionMemory / stageCompleted.stageInfo.numTasks))
      }
    }
    spark.sparkContext.addSparkListener(listener)

    // defines some useful column functions
    val blank = (c: String) => concat(lit("_:"), md5(col(c))).as(c)
    val removeLangTag = regexp_replace(col("o"), "@[a-z]+$", "").as("o")

    // load files from parquet, only these datasets are pre-processed
    val labelTriples = readParquet(s"$base/$release/$dataset/labels.parquet").where($"lang".isin(languages: _*))
    val allInfoboxTriples = readParquet(s"$base/$release/$dataset/infobox_properties.parquet").where($"lang".isin(languages: _*))
    val interlangTriples = readParquet(s"$base/$release/$dataset/interlanguage_links.parquet").where($"lang".isin(languages: _*))
    val categoryTriples = readParquet(s"$base/$release/$dataset/article_categories.parquet").where($"lang".isin(languages: _*))
    val infoboxTriples = topInfoboxPropertiesPerLang.foldLeft(allInfoboxTriples){ case (triples, topk) =>
      // get the top-k most frequent properties per language
      val topkProperties =
        triples
          .groupBy($"p", $"lang").count()
          .withColumn("k", row_number() over Window.partitionBy($"lang").orderBy($"count".desc, $"p"))
          .where($"k" <= topk)
          .select($"p", $"lang")
          .cache()

      // filter triples for top-k most frequent properties per language
      triples
        .join(topkProperties, Seq("p", "lang"), "left_semi")
        .as[Triple]
    }

    // print some stats
    if (printStats) {
      val stats = Seq(
        "labels" -> labelTriples,
        "infobox_properties" -> allInfoboxTriples,
        "interlanguage_links" -> interlangTriples,
        "article_categories" -> categoryTriples
      ) ++ topInfoboxPropertiesPerLang.map(topK =>
        Seq(s"top $topK infobox_properties" -> infoboxTriples)
      ).getOrElse(Seq.empty[(String, DataFrame)])
      println()

      import spark.implicits._
      val langStats = stats.map { case (label, df) =>
        println(s"$label: ${df.count} triples, ${df.select($"s").distinct().count} nodes, ${df.select($"p").distinct().count} predicates")
        df.groupBy($"lang").count.withColumnRenamed("count", label)
      }.foldLeft(Seq.empty[String].toDF("lang")) { case (f, df) => f.join(df, Seq("lang"), "full_outer") }
      langStats.show(100, false)
      println()
    }

    // define labels without language tag (if removeLanguageTags is true)
    val labels =
      labelTriples
        .conditionally(externaliseUris, _.withColumn("s", blank("s")))
        .conditionally(removeLanguageTags, _.withColumn("o", removeLangTag))

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
        .withColumn("k", row_number() over Window.partitionBy($"p").orderBy($"count".desc, $"t"))
        .where($"k" === 1)
        .select($"p", $"t")
        .cache()

    // infobox properties with most frequent data type per property
    val infobox =
      infoboxTriplesWithDataType
        .join(infoboxPropertyDataType, Seq("p", "t"), "semi_join")
        // negative years not supported by Dgraph
        .where($"t" =!= "<http://www.w3.org/2001/XMLSchema#date>" || !$"v".startsWith("\"-"))
        .conditionally(externaliseUris, _.withColumn("s", blank("s")))
        .conditionally(externaliseUris, _.withColumn("v", when($"t" === "<uri>", blank("v")).otherwise(col("v"))))
        .select($"s", $"p", when($"t" === "<uri>", $"v").otherwise(concat($"v", lit("^^"), $"t")).as("o"), $"lang")

    // interlanguage links preprocessing
    // we are only interested in links inside our set of languages
    // we look at the dbpedia urls, en links may not contain the language code in the url,
    // but we expect `db` at its place, so with `en` in languages, we also look for links with `db`
    val langs = languages ++ (if (languages.contains("en")) Seq("db") else Seq.empty[String])
    val interlang =
      interlangTriples
        .where($"o".substr(9, 2).isin(langs: _*))
        .conditionally(externaliseUris, _.withColumn("s", blank("s")))
        .conditionally(externaliseUris, _.withColumn("o", blank("o")))

    // article_categories
    val categories =
      categoryTriples
        .conditionally(externaliseUris, _.withColumn("s", blank("s")))
        .conditionally(externaliseUris, _.withColumn("o", blank("o")))

    // xid predicate
    val xid = Seq(("<xid>", "any", "string", "@index(exact)")).toDF("p", "lang", "t", "i")

    // mapping to Dgraph types
    val dgraphDataTypes = Map(
      "<uri>" -> "[uid]",
      "<http://www.w3.org/2001/XMLSchema#date>" -> "datetime",
      "<http://www.w3.org/2001/XMLSchema#double>" -> "float",
      "<http://www.w3.org/2001/XMLSchema#integer>" -> "int",
      "<http://www.w3.org/2001/XMLSchema#string>" -> "string",
//      "<http://www.w3.org/2001/XMLSchema#string>" -> (if (removeLanguageTags) "string" else "string @lang"),
    )
    // this is deterministic, but marking it non-deterministic guarantees it is executed only once per row
    val dgraphDataTypesUdf = udf(dgraphDataTypes(_)).asNondeterministic()

    // mapping to Dgraph indices
    val dgraphIndices = Map(
      "uid" -> "@reverse",
      "[uid]" -> "@reverse",
      "datetime" -> "@index(day)",
      "float" -> "@index(float)",
      "int" -> "@index(int)",
      "string" -> "@index(fulltext)",
//      "string @lang" -> "@index(fulltext)",
    )
    val dgraphIndicesUdf = udf(dgraphIndices(_)).asNondeterministic()

    // helper variable in case we are removing language tags
    val lang = if (removeLanguageTags) "" else " @lang"

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
      ).reduce(_.unionByName(_))
        // we are only interested in one line per predicate
        .distinct()
        .conditionally(externaliseUris, _.unionByName(xid))
        .sort()
        .cache()
        .coalesce(1)

    // write schema without indices
    println("writing schema.dgraph")
    predicates
      // @ and ~ not allowed in predicates in Dgraph
      .where(!$"p".contains("@") && !$"p".contains("~"))
      .writePartitionedBy(Seq("lang"), Seq("p"), Seq.empty,
        Some(Seq(concat($"p", lit(": "), $"t", lit(" .")).as("p"), $"lang"))
      )
      .mode(SaveMode.Overwrite)
      .text(s"$base/$release/$dataset/schema.dgraph")

    // write schema with indices
    println("writing schema.indexed.dgraph")
    predicates
      // @ and ~ not allowed in predicates in Dgraph
      .where(!$"p".contains("@") && !$"p".contains("~"))
      .writePartitionedBy(Seq("lang"), Seq("p"), Seq.empty,
        Some(Seq(concat($"p", lit(": "), $"t", lit(" "), $"i", lit(" .")).as("p"), $"lang"))
      )
      .mode(SaveMode.Overwrite)
      .text(s"$base/$release/$dataset/schema.indexed.dgraph")

    // get all types from the datasets
    val articlesTypes =
      Seq(
        labelTriples.select($"s", $"lang"),
        infoboxTriples.select($"s", $"lang"),
        interlangTriples.select($"s", $"lang").where($"o".substr(9, 2).isin(langs: _*)),
        interlangTriples.select($"o".as("s"), $"lang").where($"o".substr(9, 2).isin(langs: _*)),
        categoryTriples.select($"s", $"lang")
      )
        .map(_.distinct())
        .reduce(_.unionByName(_))
        .conditionally(externaliseUris, _.withColumn("s", blank("s")))
        .withColumn("p", lit("<dgraph.type>"))
        .withColumn("o", lit("\"Article\""))
    val categoryTypes =
      categoryTriples
        .select($"o".as("s"), $"lang").distinct()
        .conditionally(externaliseUris, _.withColumn("s", blank("s")))
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

    // write dgraph rdf files
    writeRdf(labels, s"$base/$release/$dataset/labels.rdf")
    writeRdf(infobox, s"$base/$release/$dataset/infobox_properties.rdf")
    writeRdf(interlang.toDF, s"$base/$release/$dataset/interlanguage_links.rdf")
    writeRdf(categories.toDF, s"$base/$release/$dataset/article_categories.rdf")
    writeRdf(types.toDF, s"$base/$release/$dataset/types.rdf")
    if (externaliseUris)
      writeRdf(externalIds.toDF, s"$base/$release/$dataset/external_ids.rdf")
    println()

    val infoboxRdf = spark.read.text(s"$base/$release/$dataset/infobox_properties.rdf")
    println(s"cleaned-up infoboxes cover ${infoboxRdf.count() * 100 / infoboxTriples.count()}% of original rows")
    println(s"memory spill: ${memSpilled.get() / 1024/1024/1024} GB  disk spill: ${diskSpilled.get() / 1024/1024/1024} GB  peak mem per host: ${peakMem.get() / 1024/1024} MB")
    val duration = (System.nanoTime() - start) / 1000000000
    println(s"finished in ${duration / 3600}h ${(duration / 60) % 60}m ${duration % 60}s")
  }

  def getLanguages(base: String, release: String, dataset: String): Seq[String] =
    new File(new File(new File(base), release), dataset)
      .listFiles().toSeq
      .filter(_.isDirectory)
      .filter(_.getName.endsWith(".parquet"))
      .flatMap(_.listFiles())
      .filter(f => f.isDirectory && f.getName.startsWith("lang="))
      .map(_.getName.substring(5))
      .distinct

  def readParquet(path: String)(implicit spark: SparkSession): Dataset[Triple] = {
    import spark.implicits._
    spark.read.parquet(path).as[Triple]
  }

  def writeRdf(df: DataFrame, path: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    println(s"writing $path")

    df
      // @ and ~ not allowed in predicates in Dgraph
      .where(!$"p".contains("@") && !$"p".contains("~"))
      // with this partitioning you get few partition files for small languages and more files for large languages
      // partition files are mostly even sized
      // partition files will be sorted by all given columns
      .writePartitionedBy(
        Seq("lang"),    // there is a lang=… sub-directory in `path` for each language
        Seq("p", "s"),  // all rows for one predicate and subject are contained in a single part-… file
        Seq("o"),       // a part-… file in the sub-directories are sorted by `p`, `s` and `o`
        // we don't want all columns of `df` to be stored in `path` but these columns
        Some(Seq(concat($"s", lit(" "), $"p", lit(" "), $"o", lit(" .")), $"lang"))
      )
      // gzip the partitions
      .option("compression", "gzip")
      // overwrite `path` completely, if it exists
      .mode(SaveMode.Overwrite)
      // write as text to `path`
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
