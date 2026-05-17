package com.example.examples

import com.example.common.Models.orderTableName
import com.example.common.SparkSessionFactory
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.actions.SparkActions

import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

object E11_OrphanFilesRemoval extends App {

  val spark = SparkSessionFactory.create("E11_OrphanFilesRemoval")

  val table = Spark3Util.loadIcebergTable(spark, orderTableName)

  val result = SparkActions.get(spark)
    .deleteOrphanFiles(table)
    .olderThan(System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(5))
    .execute()

  val orphans = result.orphanFileLocations().asScala.toSeq
  println(s"Orphan files removed: ${orphans.size}")
  orphans.foreach(println)

  spark.stop()
}
