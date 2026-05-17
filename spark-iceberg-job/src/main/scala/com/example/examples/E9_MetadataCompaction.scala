package com.example.examples

import com.example.common.Models.{Order, orderTableName}
import com.example.common.{SparkSessionFactory, TableUtils}
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.actions.SparkActions
import scala.jdk.CollectionConverters._

object E9_MetadataCompaction extends App {

  val spark = SparkSessionFactory.create("E9_MetadataCompaction")

  println("=== Manifests before compaction ===")
  spark.sql(s"SELECT path, length, added_data_files_count FROM $orderTableName.manifests").show(truncate = false)

  val table = Spark3Util.loadIcebergTable(spark, orderTableName)

  val result = SparkActions.get(spark)
    .rewriteManifests(table)
    .execute()

  println(s"Rewritten manifests: ${result.rewrittenManifests().asScala.size}")
  println(s"Added manifests:     ${result.addedManifests().asScala.size}")

  println("=== Manifests after compaction ===")
  spark.sql(s"SELECT path, length, added_data_files_count FROM $orderTableName.manifests").show(truncate = false)

  spark.stop()
}
