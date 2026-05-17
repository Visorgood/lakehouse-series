package com.example.examples

import com.example.common.Models.{Order, orderTableName}
import com.example.common.{SparkSessionFactory, TableUtils}
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.actions.SparkActions

import java.sql.Timestamp
import java.time.LocalDateTime

object E10_DataCompaction extends App {

  val spark = SparkSessionFactory.create("E10_DataCompaction")
  import spark.implicits._

  (1 to 20).foreach { i =>
    Seq(Order(i.toLong, i.toLong, "completed", BigDecimal(s"$i.15"), Timestamp.valueOf(LocalDateTime.of(2024, 1, 21, 15, 0))))
      .toDS().writeTo(orderTableName).append()
  }

  println("=== Data files before compaction ===")
  spark.sql(s"SELECT file_path, record_count, file_size_in_bytes FROM $orderTableName.files").show(truncate = false)

  val table = Spark3Util.loadIcebergTable(spark, orderTableName)

  val result = SparkActions.get(spark)
    .rewriteDataFiles(table)
    .binPack()
    .option("target-file-size-bytes", (128 * 1024 * 1024).toString)
    .execute()

  println(s"Rewritten data files: ${result.rewrittenDataFilesCount()}")
  println(s"Added data files:     ${result.addedDataFilesCount()}")
  println(s"Rewritten bytes:      ${result.rewrittenBytesCount()}")

  println("=== Data files after compaction ===")
  spark.sql(s"SELECT file_path, record_count, file_size_in_bytes FROM $orderTableName.files").show(truncate = false)

  spark.stop()
}
