package dev.minack.enrico.dgraph.dbpedia

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object DbpediaToParquetSparkApp {

  def main(args: Array[String]): Unit = {

    println("This tool writes all language datasets into a single parquet file.")
    println("From there, Spark can load the data much more quicker, as well as easier load subsets of the languages.")

    if (args.length < 3 || args.length > 5) {
      println()
      println("Please provide path to dbpedia dataset, the release and languages (two-letters code, comma separated)")
      println("Optionally provide the dataset (defaults to: core-i18n) and a comma separated list of datasets (defaults to: labels,infobox_properties,interlanguage_links,article_categories)")
      System.exit(1)
    }

    val base = args(0)
    val release = args(1)
    val languages = args(2).split(",")
    val dataset = if (args.length == 4) args(3) else "core-i18n"
    val filenames = if (args.length == 5) args(4).split(",").toSeq else Seq("labels", "infobox_properties", "interlanguage_links", "article_categories")
    val extension = ".ttl"

    val start = System.nanoTime()

    // start a local Spark session
    implicit val spark: SparkSession =
      SparkSession
        .builder()
        .master("local[*]")
        .appName("Spark Dgraph DBpedia App")
        .config("spark.local.dir", ".")
        .getOrCreate()
    import spark.implicits._

    // turn all supported files into parquet
    val dfs = filenames.map { filename =>
      val parquet = s"$base/$release/$dataset/${filename}.parquet"
      languages.map(lang =>
        readTtl(s"$base/$release/$dataset/$lang/${filename}_$lang$extension")
          .withColumn("lang", lit(lang))
      )
        .reduce(_.unionByName(_))
        // with this range partition and sort you get fewer partitions
        // for smaller languages and order within your partitions
        .repartitionByRange($"lang", $"s")
        .sort("lang", "s", "p", "o")
        .write
        .mode(SaveMode.Overwrite)
        // this partitioning allows you to read in a subset of languages efficiently
        .partitionBy("lang")
        .parquet(parquet)

      val df = spark.read.parquet(parquet)
      println(s"$filename: ${df.count} triples, ${df.select($"s").distinct().count} nodes, ${df.select($"p").distinct().count} predicates")
      df
    }

    println()
    val df = dfs.reduce(_.union(_))
    println(s"all: ${df.count} triples, ${df.select($"s").distinct().count} nodes, ${df.select($"p").distinct().count} predicates")
    val duration = (System.nanoTime() - start) / 1000000000
    println(s"finished in ${duration / 3600}h ${(duration / 60) % 60}m ${duration % 60}s")
  }

  def readTtl(path: String*)(implicit spark: SparkSession): Dataset[Triple] = {
    import spark.implicits._

    val lines = spark.read.textFile(path: _*)
    lines
      .where(!$"value".startsWith("#"))
      .map(line => line.dropRight(2).split(" ", 3))
      .select($"value"(0).as("s"), $"value"(1).as("p"), $"value"(2).as("o"))
      .as[Triple]
  }

}
