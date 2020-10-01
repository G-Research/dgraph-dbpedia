package dgraph.dbpedia

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object PartitionSparkApp {

  def main(args: Array[String]): Unit = {

    val base = "dbpedia"
    val release = "2016-10"
    val dataset = "core-i18n"

    // start a local Spark session
    implicit val spark: SparkSession =
      SparkSession
        .builder()
        .master("local[*]")
        .appName("Spark App")
        .config("spark.local.dir", ".")
        .getOrCreate()
    import spark.implicits._


    val languages = Seq("az", "de", "vi")
    languages.map(lang =>
      readTtl(s"$base/$release/$dataset/$lang/labels_$lang.ttl")
        .withColumn("lang", lit(lang))
    )
      .reduce(_.unionByName(_))
      // with this range partition and sort you get fewer partitions
      // for smaller languages and order within your partitions
      .repartitionByRange($"lang", $"s")
      .sortWithinPartitions("lang", "s", "p", "o")
      .write
      .mode(SaveMode.Overwrite)
      // this partitioning allows you to read in a subset of languages efficiently
      .partitionBy("lang")
      .parquet("tmp.parquet")
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
