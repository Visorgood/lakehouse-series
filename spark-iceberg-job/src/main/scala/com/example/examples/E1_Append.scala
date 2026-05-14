package com.example.examples

import com.example.common.Models.{Order, orderTableName}
import com.example.common.{SparkSessionFactory, TableUtils}

import java.sql.Timestamp
import java.time.LocalDateTime

object E1_Append extends App {

  val spark = SparkSessionFactory.create("E1_Append")
  import spark.implicits._

  println("=== Before append ===")
  TableUtils.showTable(spark, orderTableName)

  val newData = Seq(
    Order(4L, 104L, "completed", BigDecimal("210.00"), Timestamp.valueOf(LocalDateTime.of(2024, 1, 17,  9, 0))),
    Order(5L, 105L, "pending",   BigDecimal("75.50"),  Timestamp.valueOf(LocalDateTime.of(2024, 1, 17, 10, 0)))
  )
  newData.toDS().writeTo(orderTableName).append()

  println("=== After append ===")
  TableUtils.showTable(spark, orderTableName)

  TableUtils.showTableSnapshots(spark, orderTableName)

  spark.stop()
}
