package com.example.examples

import com.example.common.Models.{Order, orderTableName}
import com.example.common.{SparkSessionFactory, TableUtils}

import java.sql.Timestamp
import java.time.LocalDateTime

object E3_IncrementalRead extends App {

  val spark = SparkSessionFactory.create("E3_IncrementalRead")
  import spark.implicits._

  val offset = TableUtils.fetchCurrentSnapshot(spark, orderTableName)
  println(s"Offset: $offset")

  Seq(
    Order(7L, 107L, "pending", BigDecimal("95.00"), Timestamp.valueOf(LocalDateTime.of(2024, 1, 19, 14, 0))),
    Order(8L, 108L, "pending",   BigDecimal("45.00"), Timestamp.valueOf(LocalDateTime.of(2024, 1, 19, 15, 0)))
  ).toDS().writeTo(orderTableName).append()

  Seq(
    Order(9L, 109L, "pending", BigDecimal("32.00"), Timestamp.valueOf(LocalDateTime.of(2024, 1, 19, 16, 0)))
  ).toDS().writeTo(orderTableName).append()

  val increment = spark.read
    .option("start-snapshot-id", offset)
    .table(orderTableName)
    .cache()

  println(s"New rows since offset: ${increment.count()}")
  increment.show(truncate = false)

  val newOffset = TableUtils.fetchCurrentSnapshot(spark, orderTableName)
  println(s"New offset: $newOffset")

  TableUtils.showTableSnapshots(spark, orderTableName)

  spark.stop()
}
