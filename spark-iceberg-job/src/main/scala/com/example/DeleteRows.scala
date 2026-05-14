package com.example

import com.example.common.Models.orderTableName
import com.example.common.{SparkSessionFactory, TableUtils}

object DeleteRows extends App {

  val spark = SparkSessionFactory.create("DeleteRows")

  println("=== Before DELETE ===")
  TableUtils.showTable(spark, orderTableName)

  val predicate = "order_id in (10, 11, 12)"
  spark.sql(s"DELETE FROM $orderTableName WHERE $predicate")

  println("=== After DELETE ===")
  TableUtils.showTable(spark, orderTableName)

  TableUtils.showTableSnapshots(spark, orderTableName)

  spark.stop()
}
