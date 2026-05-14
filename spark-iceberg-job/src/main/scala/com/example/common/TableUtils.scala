package com.example.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object TableUtils {

  def showTable(spark: SparkSession, tableName: String): Unit = {
    spark.table(tableName).show(truncate = false)
  }

  def fetchCurrentSnapshot(spark: SparkSession, tableName: String): String = {
    spark.table(s"$tableName.snapshots")
      .orderBy(col("committed_at").desc)
      .limit(1)
      .select("snapshot_id")
      .first()
      .getLong(0)
      .toString
  }

  def showTableSnapshots(spark: SparkSession, tableName: String): Unit = {
    spark.sql(s"""
    SELECT snapshot_id,
           committed_at,
           operation,
           summary['added-records'] AS added_records,
           summary['deleted-records'] AS deleted_records,
           summary['total-records'] AS total_records
    FROM $tableName.snapshots
    ORDER BY committed_at DESC
  """).show(truncate = false)
  }
}
