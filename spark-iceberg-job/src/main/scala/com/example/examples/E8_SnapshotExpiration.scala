package com.example.examples

import com.example.common.Models.{Order, orderTableName}
import com.example.common.{SparkSessionFactory, TableUtils}
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.actions.SparkActions

object E8_SnapshotExpiration extends App {

  val spark = SparkSessionFactory.create("E8_SnapshotExpiration")

  println("=== Snapshots before expiration ===")
  TableUtils.showTableSnapshots(spark, orderTableName)

  val table = Spark3Util.loadIcebergTable(spark, orderTableName)

  val result = SparkActions.get(spark)
    .expireSnapshots(table)
    .expireOlderThan(System.currentTimeMillis())
    .retainLast(1)
    .deleteWith(_ => ())
    .execute()

  println(s"Deleted data files:          ${result.deletedDataFilesCount()}")
  println(s"Deleted manifest list files: ${result.deletedManifestListsCount()}")
  println(s"Deleted manifest files:      ${result.deletedManifestsCount()}")

  println("=== Snapshots after expiration ===")
  TableUtils.showTableSnapshots(spark, orderTableName)

  spark.stop()
}
