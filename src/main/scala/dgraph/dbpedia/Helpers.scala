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
package dgraph.dbpedia

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Dataset, Row}

object Helpers {

  trait WhenDataFrame {
    def call(transformation: DataFrame => DataFrame): DataFrame
  }

  case class ThenDataFrame[T](df: Dataset[T]) extends WhenDataFrame {
    override def call(transformation: DataFrame => DataFrame): DataFrame = df.call(transformation)
  }

  case class OtherwiseDataFrame[T](df: Dataset[T]) extends WhenDataFrame {
    override def call(transformation: DataFrame => DataFrame): DataFrame = df.toDF
  }

  implicit class ExtendedDataFrame[T](df: Dataset[T]) {

    /**
     * Executes the given transformation on the Dataset / DataFrame.
     *
     * @param transformation transformation
     * @return DataFrame after calling the transformation
     */
    def call(transformation: DataFrame => DataFrame): DataFrame = transformation(df.toDF)

    /**
     * Allows to perform a transformation on the Dataset / DataFrame if the given condition is true:
     *
     * {{{
     *   df.when(true).call(_.dropColumn("col"))
     * }}}
     *
     * @param condition condition
     * @return WhenDataFrame
     */
    def when(condition: Boolean): WhenDataFrame =
      if (condition) ThenDataFrame(df) else OtherwiseDataFrame(df)

    /**
     * Writes the Dataset / DataFrame via DataFrameWriter.partitionBy. In addition to partitionBy,
     * this method sorts the data to improve partition file size. Small partitions will contain few
     * files, large partitions contain more files. Partition ids are contained in a single partition
     * file per `partitionBy` partition only. Rows within the partition files are also sorted,
     * if partitionOrder is defined.
     *
     * Calling:
     * {{{
     *   df.writePartitionedBy(Seq("a"), Seq("b"), Seq("c"), _.select(concat($"b", $"c")))
     * }}}

     * is equivalent to:
     * {{{
     *   df.repartitionByRange($"a", $"b")
     *     .sortWithinPartitions($"a", $"b", $"c")
     *     .select(concat($"b", $"c"))
     *     .write
     *     .partitionBy("a")
     * }}}
     *
     * @param partitionBy columns used for partitioning
     * @param partitionIds columns where individual values are written to a single file
     * @param partitionOrder additional columns to sort partition files
     * @param transformation additional transformation to be applied before calling write
     * @return configured DataFrameWriter
     */
    def writePartitionedBy(partitionBy: Seq[String],
                           partitionIds: Seq[String],
                           partitionOrder: Seq[String] = Seq.empty,
                           transformation: DataFrame => DataFrame = identity): DataFrameWriter[Row] = {
      df
        // with this range partition and sort you get fewer partitions
        // for smaller languages and order within your partitions
        .repartitionByRange((partitionBy ++ partitionIds).map(col): _*)
        .sortWithinPartitions((partitionBy ++ partitionIds ++ partitionOrder).map(col): _*)
        .call(transformation)
        .write
        // the partitionBy allows you to read in a subset of languages efficiently
        .partitionBy(partitionBy: _*)
    }
  }

}
