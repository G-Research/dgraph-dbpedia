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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import uk.co.gresearch.spark._
import uk.co.gresearch._

import java.io.File

object DbpediaToParquetSparkApp {

  def main(args: Array[String]): Unit = {

    println("This tool writes all language datasets into a single parquet file.")
    println("From there, Spark can load the data much quicker, as well as easier load subsets of the languages.")

    if (args.length < 2 || args.length > 4) {
      println()
      println("Please provide path to dbpedia dataset and the release")
      println("Optionally provide the languages (two-letters code, comma separated) and a comma separated list of datasets")
      println("When no datasets are given, then all datasets are loaded into parquet files")
      System.exit(1)
    }
    println()

    val base = args(0)
    val release = args(1)
    val dataset = "core-i18n"
    val languages = if (args.length >= 3 && args(2).length > 0) args(2).split(",").toSeq else getLanguages(base, release, dataset)
    val filenames = if (args.length == 4) args(3).split(",").toSeq else getDatasets(base, release, dataset)
    val extension = ".ttl"

    println(s"Loading release $release of $dataset")
    println(s"Loading these languages: ${languages.mkString(", ")}")
    println(s"Loading these datasets: ${filenames.mkString(", ")}")
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

    // turn all files into parquet
    val dfs = filenames.map { filename =>
      val parquet = s"$base/$release/$dataset/${filename}.parquet"

      // for each language, read the ttl file and add the `lang` column
      // also read the en_uris dataset (if it exists)
      languages.map { lang =>
        val path = s"$base/$release/$dataset/$lang/${filename}_$lang$extension"
        val enUrisPath = s"$base/$release/$dataset/$lang/${filename}_en_uris_$lang$extension"

        readTtl(path).withColumn("lang", lit(lang))
          .when(languages.contains("en") && new File(enUrisPath).exists())
          .call(_.unionByName(
            readTtl(enUrisPath).withColumn("lang", concat(lit("en-"), lit(lang)))
          ))
      }
        // union all ttl files
        .reduce(_.unionByName(_))
        // write all data partitioned by language `lang` and sorted by `s`, `p` and `o`
        // with this partitioning you get few partition files for small languages and more files for large languages
        // partition files are mostly even sized
        // partition files will be sorted by all given columns
        .writePartitionedBy(
          Seq($"lang"),   // there is a lang=… sub-directory in `path` for each language
          Seq($"s"),      // all rows for one subject is contained in a single part-… file
          Seq($"p", $"o")  // the part-… files are sorted by `s`, `p` and `o`
        )
        .mode(SaveMode.Overwrite)
        .parquet(parquet)

      // read from parquet file to print some stats
      val df = spark.read.parquet(parquet)
      println(f"$filename: ${df.count}%,d triples, ${df.select($"s").distinct().count}%,d nodes, ${df.select($"p").distinct().count}%,d predicates")
      df
    }
    println()

    // print overall statistics
    val df = dfs.foldLeft(spark.emptyDataset[Triple].withColumn("lang", lit("")))(_.union(_))
    println(f"all: ${df.count}%,d triples, ${df.select($"s").distinct().count}%,d nodes, ${df.select($"p").distinct().count}%,d predicates")
    val duration = (System.nanoTime() - start) / 1000000000
    println(s"finished in ${duration / 3600}h ${(duration / 60) % 60}m ${duration % 60}s")

    spark.stop()
  }

  def getLanguages(base: String, release: String, dataset: String): Seq[String] =
    Option(new File(new File(new File(base), release), dataset).listFiles())
      .map(_.toSeq)
      .getOrElse(Seq.empty)
      .filter(_.isDirectory)
      .map(_.getName)
      .filter(n => n.length == 2 || n.length == 3)

  def getDatasets(base: String, release: String, dataset: String): Seq[String] = {
    Option(new File(new File(new File(base), release), dataset).listFiles())
      .map(_.toSeq)
      .getOrElse(Seq.empty)
      .filter(_.isDirectory)
      .filter(n => n.getName.length == 2 || n.getName.length == 3)
      .flatMap(_.listFiles())
      .filter(f => Option(f).exists(_.isFile))
      .map(_.getName)
      .filter(_.endsWith(".ttl"))
      .map(n => n.substring(0, n.lastIndexOf("_")))
      .filter(!_.endsWith("_en_uris"))
      .distinct
      .sorted
  }

  def readTtl(paths: String*)(implicit spark: SparkSession): Dataset[Triple] = {
    import spark.implicits._

    spark
      // read the ttl file as a text file
      .read.textFile(paths: _*)
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

}
