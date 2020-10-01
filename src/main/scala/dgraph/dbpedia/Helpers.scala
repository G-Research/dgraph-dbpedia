package dgraph.dbpedia

import org.apache.spark.sql.{Column, DataFrame, DataFrameWriter, Dataset, Row}
import org.apache.spark.sql.functions.col

object Helpers {

  implicit class ConditionalDataFrame[T](df: Dataset[T]) {
    def conditionally(condition: Boolean, method: DataFrame => DataFrame): DataFrame =
      if (condition) method(df.toDF) else df.toDF
  }

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

}
