/*
 * Copyright 2020 G-Research
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.gresearch.dgraph.dbpedia

import uk.co.gresearch.spark._
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, FloatType}
import org.apache.spark.sql._

import java.io.File
import java.util.concurrent.atomic.AtomicLong

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
    val languages = if (args.length == 3) getLanguages(args(2)) else None
    val writeTypes = false  // this is very expensive, only do if you need it
    val externaliseUris = false
    val removeLanguageTags = false
    // set to None to get all infobox properties, or Some(100) to get top 100 infobox properties
    val topInfoboxPropertiesPerLang = Some(100)
    val printStats = true

    println(s"Pre-processing release $release of $dataset")
    println(s"Pre-processing these languages: ${languages.map(_.mkString(", ")).getOrElse("all")}")
    if (writeTypes)
      println("Writing type information")
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

    // we look at the object dbpedia urls, en links may not contain the language code in the url,
    // but we expect `dbpedia` at its place, so with `en` in languages, we also look for links with `dbpedia`
    def getObjectLanguages(allLanguages: Dataset[String]): Seq[String] =
      languages
        .orElse(Some(allLanguages.collect().toSeq))
        .map(l => l ++ (if (l.contains("en")) Seq("dbpedia") else Seq.empty[String]))
        .get

    // load files from parquet, only these datasets are being pre-processed
    val path = s"$base/$release/$dataset"
    val labelTriples = readParquet(path, "labels", languages)
    val allInfoboxTriples = readParquet(path, "infobox_properties", languages)
    val allInterlangTriples = readParquet(path, "interlanguage_links", languages)
    // we are only interested in links inside our set of languages
    val objectLangs = getObjectLanguages(allInterlangTriples.select($"lang").distinct.as[String])

    // these are deterministic, but marking them non-deterministic guarantees they are executed only once per row
    val getNodeLangUdf = udf((uri: String) => uri.split("\\.")(0).substring(8)).asNondeterministic()

    val interlangTriples = allInterlangTriples.where(getNodeLangUdf($"o").isin(objectLangs: _*))
    val pageLinksTriples = readParquet(path, "page_links", languages)
    val categoryTriples = readParquet(path, "article_categories", languages)
    val skosTriples = readParquet(path, "skos_categories", languages)
    // we derive geo values from one predicate only
    val geoTriples = readParquet(path, "geo_coordinates", languages).where($"p" === "<http://www.georss.org/georss/point>")
    val infoboxTriples = topInfoboxPropertiesPerLang.foldLeft(allInfoboxTriples){ case (triples, topk) =>
      // get the top-k most frequent properties per language (ignore en-* languages)
      val topkProperties =
        triples.where(!$"lang".contains("-"))
          .groupBy($"p", $"lang").count()
          .withColumn("k", row_number() over Window.partitionBy($"lang").orderBy($"count".desc, $"p"))
          .where($"k" <= topk)
          .select($"p", $"lang")
          .cache()

      // filter triples for top-k most frequent properties per language
      // take top-k en properties from en-{lang} datasets
      triples
        .withColumn("node-lang", when($"lang".contains("-"), "en").otherwise($"lang"))
        .join(topkProperties.withColumn("node-lang", $"lang"), Seq("p", "node-lang"), "left_semi")
        .as[Triple]
    }

    // print some stats
    if (printStats) {
      val stats = Seq(
        "labels" -> labelTriples,
        "interlanguage_links" -> interlangTriples,
        "page_links" -> pageLinksTriples,
        "article_categories" -> categoryTriples,
        "skos_categories" -> skosTriples,
        "geo_coordinates" -> geoTriples,
        "infobox_properties" -> allInfoboxTriples
      ) ++ topInfoboxPropertiesPerLang.map(topK =>
        Seq(s"top $topK infobox_properties" -> infoboxTriples)
      ).getOrElse(Seq.empty[(String, DataFrame)])

      import spark.implicits._
      val langStats = stats.map { case (label, df) =>
        println(f"$label: ${df.count}%,d triples, ${df.select($"s").distinct().count}%,d nodes, ${df.select($"p").distinct().count}%,d predicates")
        df.groupBy($"lang").count.withColumnRenamed("count", label)
      }.foldLeft(Seq.empty[String].toDF("lang")) { case (f, df) => f.join(df, Seq("lang"), "full_outer") }
        .cache
      println()

      println("Triples per languages and dataset:")
      // print all languages and aggregate all en-* langs
      langStats
        .where(!$"lang".startsWith("en-"))
        .union(
          langStats
            .where($"lang".startsWith("en-"))
            .withColumn("lang", lit("en-*"))
            .groupBy($"lang")
            .sum()
        )
        .orderBy($"lang")
        .show(1000, false)
      // print all en-* langs
      langStats
        .where($"lang".startsWith("en-"))
        .orderBy($"lang")
        .show(1000, false)
    }

    // define labels without language tag (if removeLanguageTags is true)
    val labels =
      labelTriples
        .when(externaliseUris).call(_.withColumn("s", blank("s")))
        .when(removeLanguageTags).call(_.withColumn("o", removeLangTag))

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
        .join(infoboxPropertyDataType, Seq("p", "t"), "left_semi")
        // negative years not supported by Dgraph
        .where($"t" =!= "<http://www.w3.org/2001/XMLSchema#date>" || !$"v".startsWith("\"-"))
        .when(externaliseUris).call(_.withColumn("s", blank("s")))
        .when(externaliseUris).call(_.withColumn("v", when($"t" === "<uri>", blank("v")).otherwise(col("v"))))
        .select($"s", $"p", when($"t" === "<uri>", $"v").otherwise(concat($"v", lit("^^"), $"t")).as("o"), $"lang")

    // interlanguage links preprocessing
    val interlang =
      interlangTriples
        .when(externaliseUris).call(_.withColumn("s", blank("s")))
        .when(externaliseUris).call(_.withColumn("o", blank("o")))

    // page_links
    val pageLinks =
      pageLinksTriples
        .when(externaliseUris).call(_.withColumn("s", blank("s")))
        .when(externaliseUris).call(_.withColumn("o", blank("o")))

    // article_categories
    val categories =
      categoryTriples
        .when(externaliseUris).call(_.withColumn("s", blank("s")))
        .when(externaliseUris).call(_.withColumn("o", blank("o")))

    // skos_categories
    val skosCategories =
      skosTriples
        .when(externaliseUris).call(_.withColumn("s", blank("s")))
        .when(externaliseUris).call(_.withColumn("o", when($"p" === "<http://www.w3.org/2004/02/skos/core#prefLabel>", $"o").otherwise(blank("o"))))
        .when(removeLanguageTags).call(_.withColumn("o", when($"p" === "<http://www.w3.org/2004/02/skos/core#prefLabel>", removeLangTag).otherwise($"o")))

    def swap(column: Column): Column = array(column(1), column(0))

    // geo_coordinates
    val geoCoordinates =
      geoTriples
        .withColumn("point", regexp_replace($"o", "\"", ""))
        // we need to swap the coordinates, the dbpedia dataset uses latitude/longitude, dgraph uses longitude/latitude order
        .withColumn("coordinates", swap(split($"point", " ").cast(ArrayType(FloatType))))
        .withColumn("json", to_json(struct(lit("Point").as("type"), $"coordinates")))
        .withColumn("o", regexp_replace($"json", "\"", "\\\\\""))
        .withColumn("o", concat(lit("\""), $"o", lit("\"^^<geo:geojson>")))
        .when(externaliseUris).call(_.withColumn("s", blank("s")))

    // xid predicate
    val xid = Seq(("external_ids", "<xid>", "any", "string", "@index(exact)")).toDF("dataset", "p", "lang", "t", "i")

    // mapping to Dgraph types
    val dgraphDataTypes = Map(
      "<uri>" -> "[uid]",
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
      "[uid]" -> "@reverse",
      "datetime" -> "@index(day)",
      "float" -> "@index(float)",
      "int" -> "@index(int)",
      "string" -> "@index(fulltext)",
    )
    val dgraphIndicesUdf = udf(dgraphIndices(_)).asNondeterministic()

    // helper variable in case we are removing language tags
    val lang = if (removeLanguageTags) "" else " @lang"

    // define all predicates from our datasets
    // for each dataset we provide: `p`: the predicate, `lang`: its language, `t`: its Dgraph data type, `i`: indices
    val predicates =
      Seq(
        Seq(
          // labels dataset has a single predicate
          ("labels", "<http://www.w3.org/2000/01/rdf-schema#label>", "any", s"string${lang}", "@index(fulltext)"),

          // categories dataset has a single predicate
          ("article_categories", "<http://purl.org/dc/terms/subject>", "any", "[uid]", "@reverse"),

          // skos categories has these four predicates
          ("skos_categories", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "any", "uid", "@reverse"),
          ("skos_categories", "<http://www.w3.org/2004/02/skos/core#prefLabel>", "any", s"string${lang}", "@index(fulltext)"),
          ("skos_categories", "<http://www.w3.org/2004/02/skos/core#related>", "any", "[uid]", "@reverse"),
          ("skos_categories", "<http://www.w3.org/2004/02/skos/core#broader>", "any", "[uid]", "@reverse"),

          // interlanguage links dataset has a single predicate
          ("interlanguage_links", "<http://www.w3.org/2002/07/owl#sameAs>", "any", "[uid]", "@reverse"),

          // page links has this predicate
          ("page_links", "<http://dbpedia.org/ontology/wikiPageWikiLink>", "any", "[uid]", "@reverse"),

          // geo coordinates dataset has this predicate
          ("geo_coordinates", "<http://www.georss.org/georss/point>", "any", "geo", "@index(geo)")
        ).toDF("dataset", "p", "lang", "t", "i"),

        // infobox properties data type and index depends on their data type `t`
        // infobox properties from en-* languages have en properties
        infoboxTriples
          .join(infoboxPropertyDataType, "p")
          .withColumn("t", dgraphDataTypesUdf($"t"))
          .withColumn("lang", when($"lang".contains("-"), "en").otherwise($"lang"))
          .select(lit("infobox_properties").as("dataset"), $"p", $"lang", $"t", dgraphIndicesUdf($"t").as("i"))
          .distinct(),
      ).reduce(_.unionByName(_))
        .distinct()
        .when(externaliseUris).call(_.unionByName(xid))
        .sort()
        .cache()
        .coalesce(1)

    // write schema without indices
    val schemaPath = s"$base/$release/$dataset/schema.dgraph"
    print(s"writing $schemaPath")
    predicates
      // @ and ~ not allowed in predicates in Dgraph
      .where(!$"p".contains("@") && !$"p".contains("~"))
      .writePartitionedBy(
        Seq($"dataset", $"lang"),
        Seq($"p"),
        Seq.empty,
        None,
        // turn columns into schema line: "$p: $t $i .",
        // e.g. "<http://de.dbpedia.org/property/typ>: string @index(fulltext) ."
        Some(Seq(concat($"p", lit(": "), $"t", lit(" .")).as("p"), $"dataset", $"lang"))
      )
      .mode(SaveMode.Overwrite)
      .text(schemaPath)
    if (printStats)
      print(f": ${spark.read.text(schemaPath).count()}%,d lines")
    println

    // write schema with indices
    val schemaIndexedPath = s"$base/$release/$dataset/schema.indexed.dgraph"
    print(s"writing $schemaIndexedPath")
    predicates
      // @ and ~ not allowed in predicates in Dgraph
      .where(!$"p".contains("@") && !$"p".contains("~"))
      .writePartitionedBy(
        Seq($"dataset", $"lang"),
        Seq($"p"),
        Seq.empty,
        None,
        // turn columns into schema line: "$p: $t $i .",
        // e.g. "<http://de.dbpedia.org/property/typ>: string @index(fulltext) ."
        Some(Seq(concat($"p", lit(": "), $"t", lit(" "), $"i", lit(" .")).as("p"), $"dataset", $"lang"))
      )
      .mode(SaveMode.Overwrite)
      .text(schemaIndexedPath)
    if (printStats)
      print(f": ${spark.read.text(schemaIndexedPath).count()}%,d lines")
    println

    val externalIds =
      Seq(
        labelTriples.select($"s", $"lang"),
        infoboxTriples.select($"s", $"lang"),
        interlangTriples.select($"s", $"lang"),
        interlangTriples.select($"o".as("s"), $"lang"),
        pageLinksTriples.select($"s", $"lang"),
        pageLinksTriples.select($"o".as("s"), $"lang"),
        categoryTriples.select($"s", $"lang"),
        categoryTriples.select($"o".as("s"), $"lang"),
        skosTriples.select($"s", $"lang"),
        skosTriples.select($"o".as("s"), $"lang").where($"p".isin("<http://www.w3.org/2004/02/skos/core#related>", "<http://www.w3.org/2004/02/skos/core#broader>")),
        // skosTriples contains this rdf type, externalize it as well
        Seq(("<http://www.w3.org/2004/02/skos/core#Concept>", "any")).toDF("s", "lang"),
        geoTriples.select($"s", $"lang")
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
    val labelsRdf = writeRdf(labels, s"$base/$release/$dataset/labels.rdf", printStats)
    val infoboxRdf = writeRdf(infobox, s"$base/$release/$dataset/infobox_properties.rdf", printStats)
    val interlangRdf = writeRdf(interlang.toDF, s"$base/$release/$dataset/interlanguage_links.rdf", printStats)
    val pageLinksRdf = writeRdf(pageLinks.toDF, s"$base/$release/$dataset/page_links.rdf", printStats)
    val categoryRdf = writeRdf(categories.toDF, s"$base/$release/$dataset/article_categories.rdf", printStats)
    val skosRdf = writeRdf(skosCategories.toDF, s"$base/$release/$dataset/skos_categories.rdf", printStats)
    val geoRdf = writeRdf(geoCoordinates.toDF, s"$base/$release/$dataset/geo_coordinates.rdf", printStats)

    if (writeTypes) {
      // get all types from the rdf files
      val articlesTypes =
        Seq(
          labelsRdf.select($"s", $"lang"),
          infoboxRdf.select($"s", $"lang"),
          interlangRdf.select($"s", $"lang"),
          interlangRdf.select($"o".as("s"), $"lang"),
          pageLinksRdf.select($"s", $"lang"),
          categoryRdf.select($"s", $"lang"),
          geoRdf.select($"s", $"lang")
        )
          .map(_.distinct())
          .reduce(_.unionByName(_))
          .withColumn("p", lit("<dgraph.type>"))
          .withColumn("o", lit("\"Article\""))
      val categoryTypes =
        categoryRdf
          .select($"o".as("s"), $"lang").distinct()
          .withColumn("p", lit("<dgraph.type>"))
          .withColumn("o", lit("\"Category\""))
      val skosTypes =
        skosRdf
          .select($"s", $"lang")
          .withColumn("p", lit("<dgraph.type>"))
          .withColumn("o", lit("\"Concept\""))
      val types =
        // no need to externalise the uris here, we read them from parquet, they are externalised already
        articlesTypes
          .unionByName(categoryTypes)
          .unionByName(skosTypes)
          .distinct()
      writeRdf(types.toDF, s"$base/$release/$dataset/types.rdf", printStats)
    }

    if (externaliseUris)
      writeRdf(externalIds.toDF, s"$base/$release/$dataset/external_ids.rdf", printStats)

    println()

    println(s"cleaned-up infoboxes cover ${infoboxRdf.count() * 100 / math.max(infoboxTriples.count(), 1)}% of original rows")
    println(s"memory spill: ${memSpilled.get() / 1024/1024/1024} GB  disk spill: ${diskSpilled.get() / 1024/1024/1024} GB  peak mem per host: ${peakMem.get() / 1024/1024} MB")
    val duration = (System.nanoTime() - start) / 1000000000
    println(s"finished in ${duration / 3600}h ${(duration / 60) % 60}m ${duration % 60}s")

    spark.stop()
  }

  def getLanguages(langs: String): Option[Seq[String]] = {
    val languages =
      Some(langs.split(",").toSeq.filter(_.nonEmpty))
        .filter(_.nonEmpty)

    languages
      .map(ls =>
        Some(ls)
          .filter(_.contains("en"))
          .getOrElse(Seq.empty)
          .filterNot(_.equals("en"))
          .map(l => s"en-$l")
          ++ ls
      )
      .map(_.sorted)
  }

  def readParquet(path: String, dataset: String, languages: Option[Seq[String]])(implicit spark: SparkSession): Dataset[Triple] = {
    import spark.implicits._
    readParquet(s"$path/${dataset}.parquet")
      .when(languages.isDefined)
      .call(_.where($"lang".isin(languages.get: _*)))
      .as[Triple]
  }

  def readParquet(path: String)(implicit spark: SparkSession): Dataset[Triple] = {
    import spark.implicits._

    if(new File(path).exists())
      spark.read.parquet(path).as[Triple]
    else
      spark.emptyDataset[Triple].withColumn("lang", lit("")).as[Triple]
  }

  def writeRdf(df: DataFrame, path: String, printStats: Boolean)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    print(s"writing $path")

    if (!df.isEmpty) {
      df
        // @ and ~ not allowed in predicates in Dgraph
        .where(!$"p".contains("@") && !$"p".contains("~"))
        // with this partitioning you get few partition files for small languages and more files for large languages
        // partition files are mostly even sized
        // partition files will be sorted by all given columns
        .writePartitionedBy(
          Seq($"lang"),     // there is a lang=… sub-directory in `path` for each language
          Seq($"p", $"s"),  // all rows for one predicate and subject are contained in a one part-… file
          Seq($"o"),        // the part-… files in the sub-directories are sorted by `p`, `s` and `o`
          None,
          // we don't want all columns of `df` to be stored in `path`, only these columns
          Some(Seq(concat($"s", lit(" "), $"p", lit(" "), $"o", lit(" .")), $"lang"))
        )
        // gzip the partitions
        .option("compression", "gzip")
        // overwrite `path` completely, if it exists
        .mode(SaveMode.Overwrite)
        // write as text to `path`
        .text(path)

      if (printStats)
        print(f": ${spark.read.text(path).count()}%,d triples")
    }
    println

    readRdf(path)
  }

  def readRdf(path: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    if(new File(path).exists())
      spark
      // read the rdf file as a text file
      .read.text(path).as[(String, String)]
      // remove the last two characters (' .') from the ttl lines
      // and split at the first two spaces (three columns: subject, predicate, object)
      .map{ case (triple, lang) => (triple.dropRight(2).split(" ", 3), lang) }
      // get the three columns `s`, `p` and `o`, and `lang`
      .select($"_1"(0).as("s"), $"_1"(1).as("p"), $"_1"(2).as("o"), $"_2".as("lang"))
    else
      spark
        .emptyDataset[Triple]
        .withColumn("lang", lit(""))
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
